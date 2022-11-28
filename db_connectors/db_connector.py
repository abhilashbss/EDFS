import mysql
import pymongo

# This is for datanode connector
class db_connector:
    def __init__(self):
        self.connector = None

    '''
    {
        'db_type':
        'db_connection':
        'db_user':
        'db_pass':
    }
    '''
    def get_connector(self, conf):
        if conf["db_type"] == "mongoDB":
            self.connector = mongo_connector(conf["db_url"])
        if conf["db_type"] == "mysql":
            self.connector = mysql_connector(conf["db_url"])

        return self.connector
'''
import pymongo
'''
class mongo_connector():

    def __init__(self, datanode_db_url):
        print(datanode_db_url)
        myclient = pymongo.MongoClient(datanode_db_url)
        self.mydb = myclient["Datanode"]

    def read(self, partition_table ):
        mycol = self.mydb[partition_table]
        data = []
        for x in mycol.find({}):
            data.append(x)
        return data

    def write(self, partition_table , partitioned_data_array):
        print(partitioned_data_array["file_content"])
        mycol = self.mydb[partition_table]
        mycol.insert_many(partitioned_data_array["file_content"])

'''
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM customers")
    myresult = mycursor.fetchall()
'''
class mysql_connector():
    def __init__(self, connection_conf):
        self.mydb = mysql.connector.connect(
            host=connection_conf["db_url"],
            user=connection_conf["db_user"],
            password=connection_conf["db_pass"],
            database=connection_conf["db_database"])
    def read(self, partition_table):
        mycursor = self.mydb.cursor()
        mycursor.execute("SELECT * FROM "+partition_table)
        myresult = mycursor.fetchall()
        data = []
        for x in myresult:
            data.append(x)
        return data

    def write(self, partition_table, partition_data):
        data = partition_data["file_content"]
        
