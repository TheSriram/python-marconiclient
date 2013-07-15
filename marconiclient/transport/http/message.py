# Copyright (c) 2013 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Implements a message controller that understands Marconi messages."""
import six


def _args_from_dict(msg):
    return {
        'href': msg['href'],
        'ttl': msg['ttl'],
        'age': msg['age'],
        'body': msg['body']
    }


def from_dict(msg, connection=None):
    """from_dict :: dict -> connection -> Message.
    :param msg: A dictionary created by decoding a Marconi message JSON reply
    :param connection: A connection to a Marconi server.
    :raises: KeyError If msg is missing fields
    :raises: TypeError if msg is not a dict.
    """
    args = _args_from_dict(msg)
    return Message(connection, **args)


class Message(object):
    """A handler for Marconi server Message resources.
    Attributes are only downloaded once - at creation time.
    """
    # note(cpp-cabrera): limit slots to reduce memory footprint
    __slots__ = ['_connection', 'href', 'ttl', 'age', 'body']

    def __init__(self, connection=None, **kwargs):
        self._connection = connection
        for k, v in six.iteritems(kwargs):
            setattr(self, k, v)

    def __repr__(self):
        return '<Message ttl:%s>' % (self.ttl,)

    def reload(self):
        """Queries the server and updates all local attributes
        with new values.
        """
        resp = self._connection.get(self.href)
        args = _args_from_dict(resp.json())
        for k, v in six.iteritems(args):
            setattr(self, k, v)

    def delete(self):
        """Deletes this resource from the server, but leaves the local
        object intact.
        """
        self._connection.delete(self.href)
