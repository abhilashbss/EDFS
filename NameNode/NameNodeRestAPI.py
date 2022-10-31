from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
from flask.ext.jsonpify import jsonify

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
api = Api(app)
Da

class Ls:
    def get(self, file_path):
        conn = db_connect.connect()
        #do something with conn and get ls
        return {}




api.add_resource(Ls, '/edfs/ls')  # Route_1


if __name__ == '__main__':
    app.run(port='5002')