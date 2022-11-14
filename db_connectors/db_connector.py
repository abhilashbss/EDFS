import mysql
import pymongo


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
        if conf["db_type"] == "mongo":
            self.connector = mongo_connector(conf)
        if conf["db_type"] == "mysql":
            self.connector = mysql_connector(conf)

'''
import pymongo


'''
class mongo_connector():

    def __init__(self, connection_conf):
        myclient = pymongo.MongoClient(connection_conf["db_url"])
        self.mydb = myclient[connection_conf["db_database"]]

    def read(self, partition_table ):
        mycol = self.mydb[partition_table]
        data = []
        for x in mycol.find({}):
            data.append(x)
        return data

    def write(self, ):


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
        
