import requests
import json


class Consumer:
    def __init__(self, manager_url, queue_id, balance_after=10, load_balancing_strategy='size'):
        self.manager_host, self.manager_port = manager_url
        self.queue_id = queue_id
        # access_token, refresh_token = self.login()
        # self.access_token = access_token
        # self.refresh_token = refresh_token
        self.load_balancing_strategy = load_balancing_strategy
        self.__current_node = {}
        self.__current_request = 0
        self.__balance_after = balance_after

    def get_statistics(self):
        response = requests.get(self.manager_host + ':' + self.manager_port + '/statistics/')
        return response.json()

    def rebalance(self):
        if self.load_balancing_strategy == 'cpu':
            return self.balance_by_cpu()

        return self.balance_by_size()

    def balance_by_cpu(self):
        statistics = self.get_statistics()
        current_node = None
        for node in statistics['data_nodes']:
            if not current_node:
                current_node = node
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
                if queue_id == self.queue_id and queue['size'] >= current_queue['size']:
                    current_node = node
        self.__current_node = current_node
        return self.__current_node

    def consume(self):
        if self.__current_request == self.__balance_after or not self.__current_node:
            self.balance_by_size()

        host = self.__current_node["address"]
        port = self.__current_node["port"]
        route = '/queues/' + self.queue_id + '/message/'
        response = requests.get(host + ':' + port + route)
        if response.status_code == 200:
            self.__current_request += 1
            return response.json()
        elif response.status_code == 404:
            print('Not Found.')

    def login(self, username, password):
        credentials = {
            'username': username,
            'password': password
        }
        url = 'http://' + self.manager_host + self.manager_port
        response = requests.post(url=url, json=credentials)
        json_data = json.loads(response.text)
        return json_data['access_token'], json_data['refresh_token']