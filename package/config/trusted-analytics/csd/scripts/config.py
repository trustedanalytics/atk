# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib, re, time, argparse, os, time, sys, getpass, socket
import codecs

GOOD = '\033[92m'
WARNING = '\033[93m'
ERROR = '\033[91m'
RESET = '\033[0m'

node=socket.gethostbyname(socket.gethostname())

parser = argparse.ArgumentParser(description="Get CDH configurations for ATK")
parser.add_argument("--host", type=str, help="Cloudera Manager Host", default="localhost")
parser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
parser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
parser.add_argument("--password", type=str, help="Cloudera Manager Password", default="admin")

parser.add_argument("--config-key", type=str, help="The full type config key lv1.lvl2.lvl3...")
parser.add_argument("--service", type=str, help="The service we will search for config")
parser.add_argument("--config-group", type=str, help="The Cloudera configuration group for the service")
parser.add_argument("--config", type=str, help="The config name")


parser.add_argument("--set", type=str, help="Are we setting a value")
parser.add_argument("--value", type=str, help="The value")
parser.add_argument("--restart", type=str, help="Restart --service?", default="yes")

#if a role is given a list of hostnames with the role will be returned space deliminated
parser.add_argument("--role", type=str, help="The role name")
parser.add_argument("--hostnames", type=str, help="The role hostnames")

#set classpath
parser.add_argument("--classpath", type=str, help="Set the classpath")

args = parser.parse_args()

def color_text(text, color):
    return text

def poll_commands(service, command_name):
    """
    poll the currently running commands to find out when the config deployment and restart have finished

    :param service: service to pool commands for
    :param command_name: the command we will be looking for, ie 'Restart'
    """
    active = True
    while active:
        time.sleep(1)
        print color_text(" . ", WARNING),
        sys.stdout.flush()
        commands = service.get_commands(view="full")
        if commands:
            for c in commands:
                if c.name == command_name:
                    active = c.active
                    break
        else:
            break
    print "\n"

def get_service_names(roles):
    """
    Get all the role names. this is used when deploying configurations after updates. The role names are very long and
    look like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f55810d31'. Used by the Cloudera api to know where the
    config is going to get deployed

    :param roles: list of roles from a service. example SPARK_WORKER, SPARK_MASTER
    :return: only the service names. will list of something like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f'
    """
    return [role.name for role in roles]


def deploy_config(service, roles):
    """
    deploy configuration for the given service and roles

    :param service: Service that is going to have it's configuration deployed
    :param roles: the roles that are going to have their configuration deployed
    :return:
    """
    print "Deploying config ",
    service.deploy_client_config(*get_service_names(roles))
    poll_commands(service, "deployClientConfig")
    print color_text("Config Deployed", GOOD)


def restart_service(service):
    """
    restart the service
    :param service: service we are going to restart

    """
    if args.restart.strip().lower() == "yes":
        print "Restarting " + service.name,
        service.restart()
        poll_commands(service, "Restart")
        print color_text("Restarted " + service.name, GOOD)

def get_role_host_names(api, roles):
    """
        get the host name for the all for the roles

    :param api: rest service handle
    :param roles: the list of service roles
    :return: list of machine host names
    """
    return [hosts.get_host(api, role.hostRef.hostId).hostname for role in roles]


def find_service_roles(roles, type):
    """
    Look for roles like spark_worker, spark_master, namenode, ... We will have duplicate roles depending on how many
    host are running the same role

    :param roles: list of service roles
    :param type: the type of role we are looking for, ie "SPARK_MASTER"
    :return: list of all roles matching the type
    """
    return [role for role in roles if role.type == type]

def find_config(groups, group_name, config_name):
    """
    find a config among all the configs in Cloudera manager for the given group

    :param groups: list of configuration groups for a service
    :param group_name: The group name we are going to be searching in
    :param config_name: the configuration we will look for
    :return: The configuration value from Cloudera manager and the corresponding configuration group
    """

    for config_group in groups:
        if config_group.name == group_name:
            for name, config in config_group.get_config(view='full').items():
                if config.name == config_name:
                    if config.value is None:
                        return config.default, config_group
                    else:
                        return config.value, config_group
    return None, None

def find_service(services, type):
    """
    Find a service handle, right now we only look for HDFS, ZOOKEEPER, and SPARK

    :param services: The list of services on the cluster
    :param type: the service we are looking for
    :return: service handle or None if the service is not found
    """
    for service in services:
        if service.type == type:
            return service
    return None

def find_exported_class_path(spark_config_env_sh):
    """
    find any current class path
    :param spark_config_env_sh: all the text from the cloudera manager spark_env.sh
    :return: the entire line containing the exported class path
    """
    return re.search('SPARK_CLASSPATH=.*', spark_config_env_sh) if spark_config_env_sh else None

def find_class_path_value(spark_config_env_sh):
    """
    find the only the class path value nothing else

    :param spark_config_env_sh: all the text from the cloudera manager spark_env.sh
    :return: found class path value
    """

    #i search all groups after the match to find the one that only has the value
    find_class_path = re.search('SPARK_CLASSPATH=(\".*\"|[^\r\n]*).*', spark_config_env_sh)
    class_path = None
    #get only the value not the 'export SPARK_CLASSPATH' chaff. find the group that only has the export value
    if find_class_path is not None:
        for g in find_class_path.groups():
            find_only_exported_value = re.search('SPARK_CLASSPATH', g)
            if find_only_exported_value is not None:
                continue
            else:
                class_path = g.strip('"')
                break
    return class_path


def create_updated_class_path(current_class_path, spark_env):
    """
    create a string with our class path addition and any other class paths that currently existed

    :param current_class_path: the current class path value
    :param spark_env: the entire spark-env.sh config text from Cloudera manager
    :return:
    """

    if current_class_path is None:
        #if no class path exist append it to the end of the spark_env.sh config
        spark_class_path = "SPARK_CLASSPATH=\"" + args.classpath_lib + "\""
        return spark_env + "\n" + spark_class_path
    else:
        #if a class path already exist search and replace the current class path plus our class path in spark_env.sh
        #config
        spark_class_path = "SPARK_CLASSPATH=\"" + current_class_path + ":" + args.classpath_lib + "\""
        return re.sub('.*SPARK_CLASSPATH=(\".*\"|[^\r\n]*).*', spark_class_path, spark_env)

def find_ia_class_path(class_path):
    """
    find any current ia class path
    :param class_path: the full class path value from Cloudera manager
    :return: found trusted analytics class path
    """
    return re.search('.*' + args.classpath_lib + '.*', class_path)


def update_spark_env(group, spark_config_env_sh):
    """
    update the park env configuration in Cloudera manager

    :param group: the group that spark_env.sh belongs too
    :param spark_config_env_sh: the current spark_env.sh value
    :return:
    """

    if spark_config_env_sh is None:
        spark_config_env_sh = ""

    #look for any current SPARK_CLASSPATH
    found_class_path = find_exported_class_path(spark_config_env_sh)

    if found_class_path is None:
        #no existing class path found
        print "No current SPARK_CLASSPATH set."

        updated_class_path = create_updated_class_path(found_class_path, spark_config_env_sh)

        print "Setting to: " + updated_class_path

        #update the spark-env.sh with our exported class path appended to whatever whas already present in spark-env.sh
        group.update_config({args.config: updated_class_path})
        return True
    else:
        #found existing classpath
        found_class_path_value = find_class_path_value(spark_config_env_sh)
        print "Found existing SPARK_CLASSPATH: " + found_class_path_value

        #see if we our LIB_PATH is set in the classpath
        found_ia_class_path = find_ia_class_path(found_class_path_value)
        if found_ia_class_path is None:
            #no existing ia classpath
            print "No existing Trusted Analytics class path found."
            updated_class_path = create_updated_class_path(found_class_path_value, spark_config_env_sh)
            print "Updating to: " + updated_class_path
            group.update_config({args.config: updated_class_path})
            return True
        else:
            #existing ia classpath
            print "Found existing Trusted Analytics class path no update needed."
            return False
    return False

cloudera_manager_host = args.host
#get the Cloudera manager host
if cloudera_manager_host is None or cloudera_manager_host == "localhost":
    try:
        #look for in the Cloudera agent config.ini file before prompting the user
        #config dir for Cloudera agent /etc/cloudera-scm-agent
        cloudera_agent_config = codecs.open(r"/etc/cloudera-scm-agent/config.ini", encoding="utf-8", mode="r")
        cloudera_manager_host = re.search('(?<=server_host=).*',cloudera_agent_config.read()).group(0)
        cloudera_agent_config.close()
    except IOError:
        print "not running on a Cloudera manager host"
        exit(1)


api = ApiResource(cloudera_manager_host, server_port=args.port, username=args.username,
                  password=args.password)

#the user picked cluster or the only cluster managed by cloudera manager
cluster = None
# Get a list of all clusters
clusters=api.get_all_clusters()

for c in clusters:
    for h in c.list_hosts():
        host = hosts.get_host(api, h.hostId)
        if host.hostname == node or host.ipAddress == node:
            cluster = c
if cluster:
    services = cluster.get_all_services()
else:
    print "Couldn't find node in any cluster"
    exit(1)
groups = None

if args.service:
    service = find_service(services, args.service)
    roles = find_service_roles(service.get_all_roles(), args.role)
    if args.hostnames:
        hostnames = get_role_host_names(api, roles)
        print " ".join(hostnames)
    else:
        groups = role_config_groups.get_all_role_config_groups(api, service.name, cluster.name)

        config, config_group = find_config(groups, args.config_group, args.config)

        if args.classpath:
            print "setting classpath"
            if update_spark_env(config_group, config) and args.restart.strip().lower() == "yes":
                deploy_config(service, roles)
                restart_service(service)
        elif args.set and args.value:
            print "settting"
            config_group.update_config({args.config: args.value})
            if args.restart.strip().lower() == "yes":
                deploy_config(service, roles)
                restart_service(service)
        else:
            print config
