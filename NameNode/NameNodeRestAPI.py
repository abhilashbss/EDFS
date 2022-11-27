from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
import ConfigParser
from db_connectors.db_connector import db_connector
from NameNodeManager import NameNodeManager

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
api = Api(app)
metastore_connector = None


def get_name_node_db_data():
    configParser = ConfigParser.RawConfigParser()
    configFilePath = './namenode.conf'
    configParser.read(configFilePath)
    config_dict = dict(configParser.items('default'))
    return config_dict["metastore_db_type"], config_dict["metastore_db_url"]

class Ls:
    def get(self, file_path):
        conn = db_connect.connect()
        # do something with conn and get ls
        return {}


api.add_resource(Ls, '/namenode')  # Route_1
api.add-resource()


if __name__ == '__main__':
    metastore_db_type, metadata_db_url = get_name_node_db_data()
    manager = NameNodeManager(metastore_db_type,metadata_db_url)


    app.run(port='5002')
