import requests
import json


class Consumer:
    def __init__(self, manager_url, queue_id, balance_after=10, load_balancing_strategy='size'):
        self.__manager_host, self.__manager_port = manager_url
        self.__queue_id = queue_id
        # access_token, refresh_token = self.login()
        # self.access_token = access_token
        # self.refresh_token = refresh_token
        self.__load_balancing_strategy = load_balancing_strategy
        self.__current_node = {}
        self.__current_request = 0
        self.__balance_after = balance_after

    def get_statistics(self):
        response = requests.get(self.__manager_host + ':' + self.__manager_port + '/statistics/')
        return response.json()

    def rebalance(self):
        if self.__load_balancing_strategy == 'cpu':
            self.balance_by_cpu()
        self.balance_by_size()

    def balance_by_cpu(self):
        statistics = self.get_statistics()
        current_node = statistics['data_nodes'][0]
        for node in statistics['data_nodes']:
            if node["cpu_load"] >= current_node["cpu_load"]:
                current_node = node
        self.__current_node = current_node
        return self.__current_node

    def balance_by_size(self):
        statistics = self.get_statistics()
        current_node = None
        for node in statistics['data_nodes']:
            if not current_node:
                current_node = node
            for queue in node['queues']:
                queue_id = queue['id']
                current_queue = list(filter(lambda x: x['id'] == queue_id, current_node['queues']))[0]
                if queue_id == self.__queue_id and queue['size'] >= current_queue['size']:
                    current_node = node
        self.__current_node = current_node
        return self.__current_node

    def consume(self):
        if self.__current_request == self.__balance_after or not self.__current_node:
            self.rebalance()

        host = self.__current_node["address"]
        port = self.__current_node["port"]
        route = '/queues/' + self.__queue_id + '/message/'
        response = requests.get(host + ':' + port + route)
        if response.status_code == 200:
            self.__current_request += 1
            return response.json()
        elif response.status_code == 404:
            print('Not Found.')
