class DatanodeInterface:
    def __init__(self, datanode_connector):
        self.datanode_connector = datanode_connector

    def ReadFilePartition(self, file_path, partition_no):
        pass

    def WriteFilePartition(self, file_path, file_partition_content, partition_table):
        pass

