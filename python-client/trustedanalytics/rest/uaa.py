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

import logging
logger = logging.getLogger(__name__)
import trustedanalytics.rest.http as http
from trustedanalytics.rest.server import Server


def get_oauth_credentials(uri, user_name, user_password):
    """
    Get the credentials needed to communicate using OAuth

    uri: URI of ATK server or the UAA server itself
    """
    try:
        oauth_info = _get_oauth_server_info(uri)  # try using ATK URI first
    except:
        oauth_info = {'uri': uri}
        logger.info("Using UAA uri %s provided explicitly." % uri)
    else:
        logger.info("Using UAA uri %s obtained from ATK server." % oauth_info['uri'])

    token_type, token, refresh_token = _get_oauth_token(oauth_info, user_name, user_password)

    return {'user': user_name,
            'oauth_uri': oauth_info['uri'],
            'token_type': token_type,
            'token': token,
            'refresh_token': refresh_token}


def get_refreshed_oauth_token(atk_uri, refresh_token):
    """
    Connect to the cloudfoundry uaa server and acquire access token using a refresh token
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token using a refresh token
    data = {'grant_type': 'refresh_token', 'refresh_token': refresh_token }
    return _get_token(_get_oauth_server_info(atk_uri), data)


# supporting objects and functions....

class UaaServer(Server):
    """Server object for communicating with the UAA server"""

    def __init__(self, uaa_info):
        scheme = Server._get_value_from_config('uaa_scheme')
        headers = Server._get_value_from_config('uaa_headers')
        super(UaaServer, self).__init__(uaa_info['uri'], scheme, headers)
        self.client_name = self._get_client_credential("client_name", uaa_info)
        self.client_password = self._get_client_credential("client_password", uaa_info)

    @staticmethod
    def _get_client_credential(credential_name, uaa_info):
        from trustedanalytics.rest.atkserver import server
        try:
            credential = getattr(server, credential_name)  # if local server object has it, use as override
        except AttributeError:
            credential = uaa_info.get(credential_name, None)  # else use uaa_info
        if credential is None:
            UaaServer.raise_bad_client_credential_error(is_missing=True)
        return credential

    @staticmethod
    def raise_bad_client_credential_error(is_missing=False):
        m = "is missing" if is_missing else "has a bad value for"
        raise RuntimeError("Authentication %s for the client app itself.  "
                           "This could mean a version mismatch or other problem with the server instance." % m)


def _get_oauth_server_info(atk_uri):
    """Get the UAA server information from the ATK Server"""
    server = _get_simple_atk_server(atk_uri)
    response = http.get(server, "oauth_server")
    server._check_response(response)
    return response.json()


def _get_simple_atk_server(atk_uri):
    """Gets ATK Server object for very simple HTTP"""
    scheme = Server._get_value_from_config('scheme')
    headers = Server._get_value_from_config('headers')
    return Server(atk_uri, scheme, headers)


def _get_oauth_token(uaa_info, user_name, user_password):
    """
    Connect to the cloudfoundry uaa server and acquire token

    Calling this method is required before invoking any ATK operations. This method connects to UAA server
    and validates the user and the client and returns an token that will be passed in the rest server header
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token by authenticating with the user's password
    data = {'grant_type': "password", 'username': user_name, 'password': user_password}
    return _get_token(uaa_info, data)


def _get_token(uaa_info, data):
    """worker to get the token tuple from the UAA server"""
    if uaa_info:
        uaa_server = UaaServer(uaa_info)
        auth = (uaa_server.client_name, uaa_server.client_password)
        response = http.post(uaa_server, "/oauth/token", auth=auth, data=data)
        try:
            uaa_server._check_response(response)
        except Exception:
            # try to provide better message if possible
            description = response.json().get('error_description', '')
            if 'No client with requested id' in description or 'Bad credentials' in description:
                UaaServer.raise_bad_client_credential_error()
            else:
                raise
        token_type = response.json()['token_type']
        token = response.json()['access_token']
        refresh_token = response.json()['refresh_token']
        return token_type, token, refresh_token
    return None, None, None
