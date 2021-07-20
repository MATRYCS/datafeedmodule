from MongoDBClient.client import MongoDBClient
from settings import PROCESSED_FILES_COLLECTION


class CollectionHandler(MongoDBClient):
    @staticmethod
    def push_file_to_collection_(collection, file_name):
        file_retrieved = collection.find_one({'name': file_name})
        if file_retrieved:
            print("exists")
            return False
        else:
            collection.insert_one({'name': file_name})
            print("does not exists")
            return True

    def append_processed_files(self, file_name):
        files_processed_collection = self.create_collection(PROCESSED_FILES_COLLECTION)
        if files_processed_collection.count_documents({}):
            status = self.push_file_to_collection_(files_processed_collection, file_name)
        else:
            files_processed_collection.insert_one({"name": file_name})
            status = True
        return status
