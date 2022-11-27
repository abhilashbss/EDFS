import pymongo

class NameNodeManager:
    def __init__(self, db_type, db_url):
        if db_type == "mongoDB":
            self.myclient = pymongo.MongoClient(db_url)
            self.mydb = self.myclient["Namenode"]
            self.mycol = self.mydb["metadata"]

    #returned object should contain datanodedb url
    def GetPartitionLocations(self, file_path):
        file_path_query = {"file_path": file_path}
        mydoc = self.mycol.find(file_path_query)
        data = list(mydoc)
        assert len(data) == 1
        return {"datanode_db_url": data[0]["datanode_db_url"], "datanode_db_type": data[0]["datanode_db_type"], "partitions": data[0]["partition_locations"]}

    #
    # create tables in datanode db and update metastore
    def CreateFilePartitions(self, file_name, file_type, number_of_partitions, datanode_type, datanode_url):
        # check if parent path is directory
        file_path_parent_query = {"file_path": "/".join(file_name.split("/")[:-1])}
        mydoc = self.mycol.find(file_path_parent_query)
        parent = list(mydoc)
        assert len(parent) == 1
        assert parent[0]["file_type"] == "directory"

        # file does not exist
        file_path_query = {"file_path": file_name}
        mydoc = self.mycol.find(file_path_query)
        file = list(mydoc)
        assert len(file) == 0

        #add the new file entry
        new_file = {"file_path": file_name,"file_type": file_type,"total_partitions": number_of_partitions,
                    "datanode_db_type": datanode_type,"datanode_db_url":datanode_url}
        partition_obj = {}
        file_name = file_name.replace("/","_")
        for i in range(number_of_partitions):
            partition_obj[str(i)] = {"table_name": file_name+"_"+str(i)}

        new_file["partition_locations"] = partition_obj
        self.mycol.insert_one(new_file)

    #
    # def Mkdir(self, parent_path, directory_name):
    #     pass
    #
    # def Ls(self, path):
    #     pass
    #
    # def Rm(self, file_path):
    #     # check if this is a directory and check the respective flags
    #     pass
    #
    # def GetDefaultDataNode(self):
    #     pass



n = NameNodeManager("mongoDB","mongodb+srv://dsci:dsci@dsci.tgtoaqs.mongodb.net/?retryWrites=true&w=majority")
# print(n.GetPartitionLocations("a/b/c"))
print(n.CreateFilePartitions("a/b/d","non-directory", 5 ,"","")) #before parent directory creation should fail, getting error


