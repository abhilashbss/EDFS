import requests


class NameNodeInterface:
    def __init__(self, nameNode_url):
        self.nameNode_url = nameNode_url

    def mkdir(self, file_path, directory_name):
        response = requests.post(self.nameNode_url, json={"command": "mkdir",
                                                          "content": {"file_path": file_path,
                                                                      "directory_name": directory_name}})
        return response.status_code

    def rm(self, file_path):
        response = requests.post(self.nameNode_url, json={"command": "rm", "content": {"filepath": file_path}})
        return response.status_code

    def create_file_partitions(self, file_path, name, no_of_partitions):
        response = requests.post(self.nameNode_url, json={"command": "create_file_partition",
                                                          "content": {"filepath": file_path,
                                                                      "file_name": name,
                                                                      "no_of_partitions": no_of_partitions}})
        return response.status_code

    def get_file_partitions(self, file_path):
        response = requests.get(self.nameNode_url + "/get_file_partitions", params={"command": "get_file_partitions",
                                                                                    "content": {"filepath": file_path}})
        return response.json()

    def ls(self, file_path):
        response = requests.get(self.nameNode_url + "/ls", params={"command": "ls",
                                                                   "content": {"filepath": file_path}})
        return response.json()

    def get_partition_table_name(self, file_path, partition_no):
        response = requests.get(self.nameNode_url + "/get_partition_table_name",
                                params={"command": "get_partition_table_name",
                                        "content": {"file_path": file_path,
                                                    "partition_no": partition_no}})
        return response.json()

    def get_default_datanode_db(self):
        response = requests.get(self.nameNode_url + "/get_default_datanode_db",
                                params={"command": "get_default_datanode_db"})
        return response.json()
