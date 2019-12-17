import requests
import json


class Consumer:
    def __init__(self, username, password, manager_url, queue_id, balance_after=10, load_balancing_strategy='size'):
        self.__manager_host, self.__manager_port = manager_url
        self.__queue_id = queue_id
        self.__username = username
        self.__password = password
        self.__access_token, self.__refresh_token = self.__login()
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
        current_node = statistics['data_nodes'][0]
        for node in statistics['data_nodes']:
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
        headers = {'access_token': self.__access_token}
        response = requests.get(host + ':' + port + route)
        url = 'http://' + self.__manager_host + ':' + self.__manager_port
        requests.post(url, headers=headers)
        if response.status_code == 200:
            self.__current_request += 1
            return response.json()
        elif response.status_code == 403:
            if self.__refresh():
                return self.consume()
            else:
                return None

    def __login(self):
        credentials = {
            'username': self.__username,
            'password': self.__password
        }
        url = 'http://' + self.__manager_host + ':' + self.__manager_port + '/login'
        response = requests.post(url=url, json=credentials).json()
        if response.status_code == 200:
            return response['access_token'], response['refresh_token']
        else:
            raise Exception('Refresh token not generated')

    def __refresh(self):
        url = 'http://' + self.__manager_host + ':' + self.__manager_port + '/refresh'
        response = requests.post(url=url, headers={'refresh_token': self.__refresh_token}).json()
        if response.status_code == 200:
            self.__refresh_token = response['refresh_token']
            self.__access_token = response['access_token']
            return True
        else:
            raise Exception('Refresh token not generated')
