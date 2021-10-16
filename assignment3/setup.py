from DbConnector import DbConnector
from pprint import pprint 
import os
from tqdm import tqdm

class dbProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
    
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_collection(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def drop_all(self):
        #flushes db
        self.drop_collection("Trackpoint")
        self.drop_collection("Activity")
        self.drop_collection("User")

    def file_len(self, file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        
        # Yield successive n-sized
        # chunks from l.
        def divide_chunks(l, n):
            
            # looping till length l
            for i in range(0, len(l), n): 
                yield l[i:i + n]
        
        # How many elements each
        # list should have
        n = 100000
        
        data_list = list(divide_chunks(docs, n))


        collection = self.db[collection_name]
        print("Inserting into %s" % collection_name)
        for data in tqdm(data_list):
            collection.insert_many(data)
        print("Insert done")        


def main():
    program = None
    try:
        program = dbProgram()
        program.create_collection(collection_name="users")
        program.show_collection()
        # program.drop_collection(collection_name='person')
        # program.drop_collection(collection_name='users')
        # Check that the table is dropped
        program.show_collection()
    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    # this is the worst error handling as it doesnt give and info of where the error occured:( caused alot of frustration last assignment...
    finally:
        if program:
            program.drop_all()
            program.connection.close_connection()


if __name__ == '__main__':
    main()
