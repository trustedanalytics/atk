#!/bin/bash

function log {
timestamp=$(date)
echo "==$timestamp: $1=="
}
#SEE IF THe db is local

#if local
#    insert access into var/lib/pgsql/data/pg_hba.conf
#    if it doesn't exist
#    start service posgres service with "service postgresql initdb"

#   then insert "host    all         " + db_username + "      127.0.0.1/32            md5 #TAINSERT\n" + pg_hba_text"

#   create db user

#   create database

#   restart postgres service

#   ---------

#restart ATK

#insert metauser
log `whoami`
local=$(echo $ATK_POSTGRES_HOST | grep "localhost\|127.0.0" )
log "local= $local"

case "$1" in
    configDB)

        if [ "$local" != "" ]; then
            log "start database configuration"
            #check if postgres config file exists
            #if it doesn't posgres hasn't been initialized
            if [ ! -f /var/lib/pgsql/data/pg_hba.conf ]; then
                log "initialize postgres: service postgresql initdb"
                service postgresql initdb
            fi
            log "insert access line into /var/lib/pgsql/data/pg_hba.conf"
            sed -i "1ihost    all         ${ATK_POSTGRES_USERNAME}      127.0.0.1/32            md5 #ATKINSERT\n" /var/lib/pgsql/data/pg_hba.conf

            log "create postgres user ${ATK_POSTGRES_USERNAME}"
            su -c "echo \"create user ${ATK_POSTGRES_USERNAME} with createdb encrypted password '${ATK_POSTGRES_PASSWORD}';\" | psql "  postgres

            log "create postgres database ${ATK_POSTGRES_DATABASE}"
            su -c "echo \"create database ${ATK_POSTGRES_DATABASE} with owner ${ATK_POSTGRES_USERNAME};\" | psql "  postgres
        fi
        ;;
    insertUser)
        log "Inserting Meta User"
        env
        if [ "$local" != "" ]; then
            existingKey=$(echo " \c ${ATK_POSTGRES_DATABASE}; \\\\  select * from users where api_key = 'test_api_key_1'; " | psql -d ${ATK_POSTGRES_DATABASE} -U ${ATK_POSTGRES_USERNAME} -w -h localhost | grep 'test_api_key_1')
            if [ "$existingKey" == "" ]; then

                echo " \c ${ATK_POSTGRES_DATABASE}; \\\\  insert into users (username, api_key, created_on, modified_on) values( 'metastore', 'test_api_key_1', now(), now() );" | psql -d ${ATK_POSTGRES_DATABASE} -U ${ATK_POSTGRES_USERNAME} -w -h localhost
                log "Inserted Meta User"
                else
                echo "Meta User was already inserted"
            fi
            else
                log "Postgres is not local"
                echo "The Meta User can only be inserted into a localally running postgresql server"
                echo "To insert the metauser loginto the remote machine running postgres and run this command"
                echo "echo \" \c ${ATK_POSTGRES_DATABASE}; \\\\\\\\  insert into users (username, api_key, created_on, modified_on) values( 'metastore', 'test_api_key_1', now(), now() );\" | psql -d ${ATK_POSTGRES_DATABASE} -U ${ATK_POSTGRES_USERNAME} -w -h localhost"
        fi
        ;;
    *)
        log "Don't understand [$1]"
        exit 2
esac