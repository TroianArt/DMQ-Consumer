from consumer import *
import unittest
from unittest import mock


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://127.0.0.1:9000/statistics/':
        statistics = {
            "data_nodes": [
                {
                    "address": "127.0.0.1",
                    "port": "5000",
                    "cpu_load": 0.6,
                    "queues": [
                        {
                            "id": "0001",
                            "name": "queue1",
                            "size": 3
                        },
                        {
                            "id": "0002",
                            "name": "queue2",
                            "size": 4
                        }
                    ]
                },
                {
                    "address": "127.0.0.1",
                    "port": "5001",
                    "cpu_load": 0.72,
                    "queues": [
                        {
                            "id": "0001",
                            "name": "queue1",
                            "size": 5
                        },
                        {
                            "id": "0002",
                            "name": "queue2",
                            "size": 6
                        }
                    ]
                }
            ]
        }
        return MockResponse(statistics, 200)

    return MockResponse(None, 404)


class TestBalancing(unittest.TestCase):
    def setUp(self):
        self._data_node = {
            "address": "127.0.0.1",
            "port": "5001",
            "cpu_load": 0.72,
            "queues": [
                {
                    "id": "0001",
                    "name": "queue1",
                    "size": 5
                },
                {
                    "id": "0002",
                    "name": "queue2",
                    "size": 6
                }
            ]
        }
        self._manager_url = ('http://127.0.0.1', '9000')
        self._queue_id = '0001'

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_size_balancing(self, get):
        example_consumer = Consumer(self._manager_url, self._queue_id)
        balanced_data_node_bu_size = example_consumer.balance_by_size()
        self.assertDictEqual(self._data_node, balanced_data_node_bu_size)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_cpu_balancing(self, get):
        example_consumer = Consumer(self._manager_url, self._queue_id, "cpu")
        balanced_data_node_by_cpu = example_consumer.balance_by_cpu()
        self.assertDictEqual(self._data_node, balanced_data_node_by_cpu)
