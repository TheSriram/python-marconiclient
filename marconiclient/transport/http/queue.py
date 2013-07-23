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

import requests
import json
import six

import marconiclient.tests.mock.message as mock_message

"""Implements a queue controller that understands Marconi queues."""

def _args_from_dict(queue):
    return {
        'name': queue['name'],
        'metadata': queue['metadata'],
        'href': queue['href']
           }


def from_dict(queue, connection=None):
    """from_dict :: dict -> connection -> Queue.
    :param msg: A dictionary created by queue's basic attributes.
    :param connection: A connection to a Marconi server.
    :raises: KeyError If queue is missing fields
    :raises: TypeError if queue is not a dict.
    """
    args = _args_from_dict(queue)
    return Queue(connection, **args)


class Queue(object):

    """This is the queue controller which will have all aspects of the queue"""
    def __init__(self, connection, **kwargs):
        self._conn = connection
        for k, v in six.iteritems(kwargs):
            setattr(self, k, v) 
    
    def name(self):
        return self.name

    def metadata(self):
        return self.metadata

    def set_metadata(self, metadata):
        self.metadata = metadata

    def stats(self):
        url = '{0}/{1}/stats'.format(self.href,self.name)
        response = self._conn.get(url)
        return response.json()

    def delete(self):
        url = '{0}/{1}'.format(self.href, self.name)
        response = self._conn.delete(url)
        return response.json()

    def claim(self, ids, claim=None, echo=False, limit=None):
        if isinstance(ids, list):
            request_ids = ""
            for single_id in ids:
                request_ids = request_ids + single_id + ','
            request_ids.strip(',')
            url = '{0}/{1}/claims?limit={2}'.format(self.href, self.name, limit)
        else:
            url = '{0}/{1}/messages'.format(
                self.href, self.name)
        response = self._conn.get(url=url)
        resplist = eval(response.json())
        for jsonobject in resplist:
            for single_id in ids:
                if single_id in jsonobject['href']: 
                    yield jsonobject

    def post_messages(self, data):
        url = '{0}/{1}/messages'.format(self.href, self.name)
        response = self._conn.post(url=url, data=json.dumps(data))
        return response.json()

    def get_messages(self,limit=None,marker=None,echo=False,ids=None):
        if marker is None and limit is None and ids is None:
            url = '{0}/{1}/messages?echo=True'.format(self.href,self.name)
        elif ids:
            if isinstance(ids, list):
                request_ids = ""
                for single_id in ids:
                    request_ids = request_ids + single_id + ','
                request_ids.strip(',')
                url = '{0}/{1}/messages?ids={2}'.format(self.href,self.name,request_ids)
            else:
                raise TypeError, "ids must be a list"
        else:
            url = '{0}/messages?marker={1}&limit={2}'.format(self.href,marker,limit)
        response = self._conn.get(url)
        resplist = eval(response.json())
        if ids:
            for jsonobject in resplist:
                yield jsonobject
        else:
            for jsonobject in resplist['messages']:
                yield jsonobject

    def update_claim(self, ttl_dict, claim_id):
        url = '{0}/{1}/claims/{2}'.format(
            self.href, self.name, claim_id)
        response = self._conn.patch(url=url, data=ttl_dict)
        return response.json()

    def release_claim(self, claim_id):
        url = '{0}/{1}/claims/{2}'.format(
            self.href, self.name, claim_id)
        response = self._conn.delete(url)
        return response.json()

