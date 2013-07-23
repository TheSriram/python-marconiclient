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

# import requests
# from sure import expect
import httpretty
import json
import mock
import unittest


import marconiclient.tests.mock.queue as mock_queue
import marconiclient.tests.mock.message as mock_message
import marconiclient.transport.http.queue as queue

QUEUE_NAME = 'AWESOME'
METADATA = {"Awesome": "True"}
URL = 'http://marconi.example.com/v1/queues/AWESOME'
MARKER = '1355-237242-783'
LIMIT = 2
CLAIM_ID = '51db7067821e727dc24df754'
# HREF = 'http://marconi.example.com/v1/queues/AWESOME/messages'
AGE = 20
TTL = 20
HREF = '/v1/queue/{0}'.format(QUEUE_NAME)



def _response(ids,claim=False):
    if claim:
        return [ mock_message.message(ids_single) for ids_single in ids]
    else:
        return { "messages" : [ mock_message.message(ids_single) for ids_single in ids] , 
                "links" : [ {"href" : HREF ,"rel":"next"} ]
                }


def _post_message_response():
    return {
  "partial": "false",
  "resources": [
    "/v1/queues/AWESOME/messages/51db6f78c508f17ddc924357",
    "/v1/queues/AWESOME/messages/51db6f78c508f17ddc924358"
  ]
}

def extract(dictionary,keys):
    return dict((key, dictionary[key]) for key in keys if key in dictionary)

def _post_message(count):
    return  [extract(dictionary,['body','ttl']) for dictionary in [mock_message.message(ttl=ttl,body=HREF) for ttl in range(0,count)]]

    # [dict((key,dictionary[key]) for key in ['body','ttl'] if key in dictionary) for dictionary in [message(ttl=ttl,body=HREF) for ttl in range(1,3)]]



def _ttl_dict():
    return {
    "ttl": "300",
    "grace": "300"
    }


class TestSimpleQueue(unittest.TestCase):

    def setUp(self):
        queue_body = {
            'name': QUEUE_NAME,
            'metadata': METADATA,
            'href': HREF
        }
        self.conn = mock.MagicMock()
        self.queue = queue.from_dict(queue_body, connection=self.conn)


    def test_get_queue_name(self):
        self.assertEqual(self.queue.name,QUEUE_NAME)

    def test_get_metadata(self):
        self.assertEqual(self.queue.metadata,METADATA)

    def test_set_metadata(self):
        self.queue.set_metadata({'Chocolate': 'Tasty'})
        self.assertEqual(self.queue.metadata,{'Chocolate': 'Tasty'})

    
    def test_get_queue_stats(self):
        new_URL = '{0}/stats'.format(URL)
        self.conn.get.return_value = mock.MagicMock()
        self.conn.get.return_value.json.return_value = json.dumps({'messages': {'free': '14629','claimed':'2409'}})
        response = self.queue.stats()
        self.assertEqual(response,json.dumps({'messages':{'free':'14629','claimed':'2409'}}))


    def test_queue_delete(self):
        self.conn.delete.return_value = mock.MagicMock()
        self.conn.delete.return_value.json.return_value = json.dumps('No Content')
        response = self.queue.delete()
        self.assertEqual(response,json.dumps('No Content'))
       

    def test_queue_claim_messages_id(self):
        new_URL = '{0}/claims?limit={1}'.format(HREF, LIMIT)
        messagelist = []
        self.conn.get.return_value = mock.MagicMock()
        self.conn.get.return_value.json.return_value = json.dumps(_response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'],claim=True))
        claimed_messages = self.queue.claim(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'],limit=LIMIT)
        for claimed_message in claimed_messages:
            messagelist.append(claimed_message)
        self.assertEqual(messagelist,eval(json.dumps(_response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'],claim=True))))

    
    def test_queue_get_messages(self):
        new_URL = '{0}/messages?echo=True'.format(URL,MARKER,LIMIT)
        messagelist = []
        self.conn.get.return_value = mock.MagicMock()
        self.conn.get.return_value.json.return_value = json.dumps(_response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02']))
        messages = self.queue.get_messages()
        messages_body = _response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'])
        for message in messages:
            messagelist.append(message)
        self.assertEqual(messagelist,eval(json.dumps(messages_body['messages'])))


    def test_queue_get_messages_id(self):
        new_URL = '{0}/messages?ids={1},{2}'.format(URL,'50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02')
        messagelist = []
        self.conn.get.return_value = mock.MagicMock()
        self.conn.get.return_value.json.return_value = json.dumps(_response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'],claim=True))
        messages = self.queue.get_messages(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'])
        messages_body = _response(ids=['50b68a50d6f5b8c8a7c62b01','50b68a50d6f5b8c8a7c62b02'],claim=True)
        for message in messages:
            messagelist.append(message)
        self.assertEqual(messagelist,eval(json.dumps(messages_body)))

    def test_queue_post_messages(self):
        new_URL = '{0}/messages'.format(URL)
        self.conn.post.return_value = mock.MagicMock()
        self.conn.post.return_value.json.return_value = json.dumps(_post_message_response())        
        messages = self.queue.post_messages(_post_message(2))
        self.assertEqual(messages,json.dumps(_post_message_response()))

    def test_queue_update_claim(self):
        new_URL = '{0}/claims/{1}'.format(URL, CLAIM_ID)
        self.conn.patch.return_value = mock.MagicMock()
        self.conn.patch.return_value.json.return_value = json.dumps('No Content')
        response = self.queue.update_claim(_ttl_dict(),CLAIM_ID)
        self.assertEqual(response,json.dumps('No Content'))

    def test_queue_release_claim(self):
        new_URL = '{0}/claims/{1}'.format(URL, CLAIM_ID)
        self.conn.delete.return_value = mock.MagicMock()
        self.conn.delete.return_value.json.return_value = json.dumps('No Content')
        response = self.queue.release_claim(CLAIM_ID)
        self.assertEqual(response,json.dumps('No Content'))

    def test_queue_delete_queue(self):
        self.conn.delete.return_value = mock.MagicMock()
        self.conn.delete.return_value.json.return_value = json.dumps('No Content')
        response = self.queue.delete()
        self.assertEqual(response,json.dumps('No Content'))
