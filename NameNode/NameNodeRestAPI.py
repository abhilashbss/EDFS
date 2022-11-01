from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
import ConfigParser
from db_connectors.db_connector import db_connector

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
api = Api(app)
metastore_connector = None


def init_name_node():
    configParser = ConfigParser.RawConfigParser()
    configFilePath = './namenode.conf'
    configParser.read(configFilePath)
    config_dict = dict(configParser.items('default'))
    return db_connector.get_connector(config_dict)


class Ls:
    def get(self, file_path):
        conn = db_connect.connect()
        # do something with conn and get ls
        return {}


api.add_resource(Ls, '/namenode')  # Route_1
api.add-resource()


if __name__ == '__main__':
    metastore_connector = init_name_node()

    app.run(port='5002')
