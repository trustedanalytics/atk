#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Connection to the Trusted Analytics REST Server
"""
import os
import requests
import json
import logging
logger = logging.getLogger(__name__)

from decorator import decorator
from trustedanalytics.core.api import api_status
import trustedanalytics.rest.config as config
from trustedanalytics.rest.server import Server
from trustedanalytics.rest.uaa import get_refreshed_oauth_token, get_oauth_credentials
from trustedanalytics.meta.installapi import install_api


def clean_file_path(file_path):
    return os.path.normpath(os.path.expandvars(os.path.expanduser(file_path)))


class InvalidAuthTokenError(RuntimeError):
    """Error raise when an invalid auth token is used"""
    pass


def _with_retry_on_token_error(function, *args, **kwargs):
    """wrapper for decorator which will refresh token on InvalidAuthTokenError"""
    try:
        return function(*args, **kwargs)
    except InvalidAuthTokenError:
        ia_server = args[0]
        if ia_server.retry_on_token_error:
            ia_server.refresh_oauth_token()
            return function(*args, **kwargs)
        else:
            raise


def with_retry_on_token_error(function):
    """decorator for http methods which call the server, opportunity for retry with a refreshed token"""
    return decorator(_with_retry_on_token_error, function)


class AtkServer(Server):
    """
    Server object for talking with ATK server

    Defaults from trustedanalytics/rest/config.py are used but
    they can be overridden by setting the values in this class.

    Properties can be overridden manually can be overridden manually

    Example::
        ta.server.uri = 'your.hostname.com'

        ta.server.ping()  # test server connection
    """

    _uri_env = 'ATK_URI'
    _creds_env = 'ATK_CREDS'

    def __init__(self):
        uri = self._get_from_env(AtkServer._uri_env, "Default ATK server URI")
        super(AtkServer, self).__init__(
            uri=uri or self._get_conf('uri'),
            scheme=self._get_conf('scheme'),
            headers=self._get_conf('headers'))
        # specific to IA Server
        self._api_version = self._get_conf('api_version')
        self._user=self._get_conf('user', None)
        self._oauth_uri = self._get_conf('oauth_uri', None)
        self._oauth_token = None
        self.oauth_refresh_token = None
        self.refresh_authorization_header()

        creds_file = self._get_from_env(AtkServer._creds_env, "Default credentials file")
        if creds_file:
            self.init_with_credentials(creds_file)
        self.retry_on_token_error = self._get_conf('retry_on_token_error', True)

    def _get_from_env(self, env_name, value_description):
        """get value from env variable.  Returns None if not found"""
        value = os.getenv(env_name, None)
        if value:
            msg = "%s taken from env $%s=%s" % (value_description, env_name, value)
            logger.info(msg)
            print msg
        return value


    def init_with_credentials(self, credentials_file):
        """initializes this server config with credentials from the given file, calls an oauth server"""
        cleaned_path = clean_file_path(credentials_file)
        with open(cleaned_path, "r") as f:
            creds = json.load(f)
        self.oauth_uri = creds.get('oauth_uri', self.oauth_uri)
        self.user = creds.get('user', self.user)
        self.oauth_token = creds.get('token', self.oauth_token)
        self.oauth_refresh_token = creds.get('refresh_token', self.oauth_token)
        self.refresh_authorization_header()

    @property
    def api_version(self):
        """API version to connect to"""
        return self._api_version

    @api_version.setter
    def api_version(self, value):
        self._error_if_conf_frozen()
        self._api_version = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        #with api_status._api_lock:
        self._error_if_conf_frozen()
        self._user = value
        self.refresh_authorization_header()

    @property
    def oauth_uri(self):
        """API version to connect to"""
        return self._oauth_uri

    @oauth_uri.setter
    def oauth_uri(self, value):
        self._error_if_conf_frozen()
        self._oauth_uri = value

    @property
    def oauth_token(self):
        return self._oauth_token

    @oauth_token.setter
    def oauth_token(self, value):
        self._oauth_token = value
        self.refresh_authorization_header()
        logger.info("Client oauth token updated to %s", self._oauth_token)

    def refresh_authorization_header(self):
        if self._oauth_token:
            self.headers['Authorization'] = self._oauth_token
        else:
            # If no oauth, we just pass the user for the token
            self.headers['Authorization'] = self.user

    def refresh_oauth_token(self):
        token_type, self.oauth_token, self.oauth_refresh_token = get_refreshed_oauth_token(self.oauth_uri, self.oauth_refresh_token)

    def _get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return super(AtkServer, self)._get_base_uri() + ("/%s" % self.api_version)

    def _check_response(self, response):
        """override base check response to check for an invalid token error"""
        if response.status_code == 400 and "CF-InvalidAuthToken" in response.text:
            raise InvalidAuthTokenError(response.text)
        super(AtkServer, self)._check_response(response)

    def ping(self):
        """
        Ping the server, throw exception if unable to connect
        """
        uri = ""
        try:
            uri = self._get_scheme_and_authority() + "/info"
            logger.info("[HTTP Get] %s", uri)
            r = requests.get(uri, timeout=config.requests_defaults.ping_timeout_secs)
            logger.debug("[HTTP Get Response] %s\n%s", r.text, r.headers)
            self._check_response(r)
            if "Trusted Analytics" != r.json()['name']:
                raise Exception("Invalid response payload: " + r.text)
            print "Successful ping to Trusted Analytics at " + uri
        except Exception as e:
            message = "Failed to ping Trusted Analytics at %s\n%s" % (uri, e)
            logger.error(message)
            raise IOError(message)

    def connect(self, credentials_file=None):
        """
        Connect to the trustedanalytics server.

        This method calls the server, downloads its API information and
        dynamically generates and adds the appropriate Python code to the
        Python package for this python session.
        Calling this method is required before invoking any server activity.

        After the client has connected to the server, the server config
        cannot be changed.
        User must restart Python in order to change connection info.

        Subsequent calls to this method invoke no action.

        There is no "connection" object or notion of being continuously
        "connected".
        The call to connect is just a one-time process to download the API
        and prepare the client.
        If the server goes down and comes back up, this client will not
        recognize any difference from a connection point of view, and will
        still be operating with the API information originally downloaded.

        Parameters
        ==========
        credentials_file: str (optional)
            File name of a credentials file.
            If supplied, it will override the settings authentication
            settings in the client's server configuration.
            The credentials file is normally obtained through the environment.
        """

        #with api_status._api_lock:
        if api_status.is_installed:
            print "Already connected.  %s" % api_status
        else:
            if credentials_file:
                self.init_with_credentials(credentials_file)
            try:
                install_api(self)
            except RuntimeError as runErr:
                if '''The resource requires authentication, which was not supplied with the request''' in str(runErr):
                    raise RuntimeError(str(runErr) + ".  Credentials must be supplied through a file specified by env "
                                       "$%s before launching python or by an argument in the call to connect().  You "
                                       "can create credentials now by running ta.create_credentials_file('<filename>') "
                                       "and then calling ta.connect('<filename>')" % AtkServer._creds_env)
                else:
                    raise
            msg = "Connected.  %s" % api_status
            logger.info(msg)
            print msg

    @staticmethod
    def _get_conf(name, default=Server._unspecified):
        return Server._get_value_from_config(name, default)

    # HTTP calls to server

    @with_retry_on_token_error
    def get(self, url):
        return super(AtkServer, self).get(url)

    @with_retry_on_token_error
    def post(self, url, data):
        return super(AtkServer, self).post(url, data)

    @with_retry_on_token_error
    def delete(self, url):
        return super(AtkServer, self).delete(url)


server = AtkServer()


# Credentials creation

class _CreateCredentialsCache(object):
    """caches the most recent user entries, to prevent retyping  (excludes password)"""
    uri=None
    user=None


def default_input(message, default):
    """get raw input from a prompt which can provide a default value if user just hits Enter"""
    return raw_input("%s%s: " % (message, '' if not default else ' [%s]' % default)) or default


def create_credentials_file(filename):
    """Runs an interactive prompt to collect user input to make a credentials file, calls oauth server for a token."""
    import json
    import getpass
    cleaned_path = clean_file_path(filename)
    with open(cleaned_path, "w"):
        pass

    cache = _CreateCredentialsCache  # alias the cache singleton for convenience
    cache.uri = default_input("URI of the ATK server", cache.uri)
    if not cache.uri:
        print "Empty URI, aborting."
        return
    cache.user = default_input("User name", cache.user)
    password = getpass.getpass()
    stars = "*" * len(password)
    try:
        credentials = get_oauth_credentials(cache.uri, cache.user, password)
    except Exception as error:
        raise RuntimeError("""
Unable to acquire oauth token with these credentials.
  URI: %s
  User name: %s
  Password: %s

  Error msg: %s
""" % (cache.uri, cache.user, stars, str(error)))

    with open(cleaned_path, "w") as f:
        f.write(json.dumps(credentials, indent=2))
    print "\nCredentials file created at '%s'" % cleaned_path

    if cache.uri != credentials['oauth_uri']:
        # indicates that cache.uri must be the ATK server uri, so we can offer to connect now
        connect_now = raw_input("Connect now? [y/N] ")
        if connect_now and connect_now[0].lower() == 'y':
            print "Attempting to connect to %s now..." % cache.uri
            server.uri = cache.uri
            server.connect(cleaned_path)
