
import NameNodeInterface
class EDFSClient:
    def __init__(self, name_node_url, metastore_type):
        metadata_db_connector = db_connectors.get_metadata_connector(name_node_url, metastore_type)
        self.nameNodeInterface = NameNodeInterface(name_node_url)

    def WriteFile(self, fs_path, local_file_path, no_of_partitions):
        self.nameNodeInterface.
        pass

    def ReadFile(self, fs_path):
        pass

    def ReadFilePartition(self, fs_path, partition_no):
        pass