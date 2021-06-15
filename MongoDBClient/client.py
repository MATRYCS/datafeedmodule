import pymongo

from settings import MONGO_URI, DATABASE_NAME


class MongoDBClient(object):
    def __init__(self):
        self.__conn = pymongo.MongoClient(MONGO_URI)
        self.db = self.__conn[DATABASE_NAME]

    def create_collection(self, collection_name):
        collection = self.db[collection_name]
        return collection

    def insert_many_(self, df, collection):
        df = df.to_dict('records')
        collection.insert_many(df)
