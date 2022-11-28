from FilePartitioner.CountBasedPartitioner import CountBasedPartitioner
from NameNodeInterface import NameNodeInterface
from db_connectors.db_connector import db_connector
from DatanodeInterface import DatanodeInterface


class EDFSClient:
    def __init__(self, name_node_config):
        # self.metadata_db_connector = db_connector.get_metadata_connector(name_node_url, metastore_type)
        self.nameNodeInterface = NameNodeInterface(name_node_config)
        self.datanode_connector = None
        datanode_config = self.nameNodeInterface.get_default_datanode_db()
        self.datanode_connector = db_connector().get_connector(datanode_config)
        self.dataNodeInterface = DatanodeInterface(self.datanode_connector)

    def WriteFile(self, fs_path, local_file_path, local_file_format, no_of_partitions):
        new_file_meta = self.nameNodeInterface.create_file_partitions(fs_path, no_of_partitions)
        partitioner = CountBasedPartitioner(local_file_path, local_file_format)
        partitioner.partition(no_of_partitions)
        partitions = partitioner.get_all_partitions()
        table_names = []
        total_partitions = new_file_meta["total_partitions"]
        for i in range(total_partitions):
            table_names.append(new_file_meta["partition_locations"][str(i)]["table_name"])

        assert len(table_names) == len(partitions)
        for i in range(len(table_names)):
            status = self.dataNodeInterface.WriteFilePartition(fs_path, partitions[i], table_names[i])
            # if not status:
            #     print(status)
            #     print("Unable to write partition "+str(i)+" to table "+table_names[i])
            #     return False
        return True

    def ReadFile(self, fs_path):
        file_partitions = self.nameNodeInterface.get_file_partitions(fs_path)
        file_data = []
        for i in range(len(file_partitions)):
            file_data.append(self.dataNodeInterface.ReadFilePartition(fs_path, file_partitions[i].partition_table_name))
        return "".join(file_data)


    def ReadFilePartition(self, fs_path, partition_no):
        self.dataNodeInterface.ReadFilePartition(fs_path, partition_no)


e = EDFSClient("/Users/abhilashbss/Desktop/repositories/EDFS/EDFS_client/namenode_config.conf")
e.WriteFile("a/b/csv","/Users/abhilashbss/Desktop/repositories/EDFS/EDFS_client/sample_text.csv",
            "csv", 4 )
# print(e.ReadFile("a/b/csv"))
# print(e.ReadFilePartition("a/b/csv",2))





