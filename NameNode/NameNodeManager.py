class NameNodeManager:
    def __init__(self, meta_db_connector):
        self.meta_db_connector = meta_db_connector

    #returned object should contain datanodedb url
    def GetPartitionLocations(self, file_path):
        pass

    # create tables in datanode db and update metastore
    def CreateFilePartitions(self, parent_path, file_name, number_of_partitions):
        pass

    def Mkdir(self, parent_path, directory_name):
        pass

    def Ls(self, path):
        pass

    def Rm(self, file_path):
        # check if this is a directory and check the respective flags
        pass

    def GetDefaultDataNode(self):
        pass
