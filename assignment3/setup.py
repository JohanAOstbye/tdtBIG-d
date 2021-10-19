from DbConnector import DbConnector
import bson
from datetime import datetime
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
        print("Dropping collection %s" % collection_name)
        collection = self.db[collection_name]
        collection.drop()

        
    def show_collection(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def drop_all(self):
        #flushes db
        self.drop_collection("trackpoints")
        self.drop_collection("activities")
        #self.drop_collection("users")

    def file_len(self, file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    
    def get_date_time(self, date_time: str):
        return datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    
    def create_user(self, id: str, labels: bool):
        return {
                    "_id": id,
                    "has_labels": labels,
                }
    
    def create_activity(self, id: int, user_id: str, transportation_mode, start_date_time: str, end_date_time: str):
        return {
                    "_id": id,
                    "user_id": user_id,
                    "transportation_mode": transportation_mode,
                    "start_date_time": start_date_time,
                    "end_date_time": end_date_time,
                }

    def create_trackpoint(self, id: int, activity_id: str, lat: float, lon: float, altitude: float, date_time: str):
        return {
                    "_id": id,
                    "activity_id": activity_id,
                    "pos": {
                        "latitude": lat, 
                        "longitude": lon, 
                        "altitude": altitude,
                    },
                    "date_time": date_time,
                }

    def init_users(self):
        self.labeled_users = [] #list of user with labels on their activities
        with open("dataset/labeled_ids.txt") as file:
            for user in file:
                user = user[:-1] #remove /n
                self.labeled_users.append(user) #storing everything in memory!
    
    def insert_documents(self, docs, collection_name):
        collection = self.db[collection_name]
        # print("Inserting into %s" % collection_name)
        # collection.insert_many(docs)
        # print("Insert done")

        # return
        # Yield successive n-sized arrays from array
        def divide_chunks(array, n):
            
            # looping till length l
            for i in range(0, len(array), n): 
                yield array[i:i + n]
        
        # How many elements each list should have
        n = 500000
        
        data_list = list(divide_chunks(docs, n))


        print("Inserting into %s" % collection_name)
        for data in tqdm(data_list):
            collection.insert_many(data)
        print("Insert done")
    
    def build_trackpoints(self, activity_id, activity_file):
        trackpoint_docs = []
        with open(activity_file) as activity:
            for x in range(6):
                next(activity) # skips the first 6 lines of the .plt file

            for trackpoint in activity:
                lat = (float)(trackpoint.split(",")[0])
                lon = (float)(trackpoint.split(",")[1])
                altitude = (float)(trackpoint.split(",")[3])
                date_time = trackpoint.split(",")[5] + " " + trackpoint.split(",")[6][:-1]
                trackpoint_doc = self.create_trackpoint(self.tp_id, activity_id, lat, lon, altitude, date_time)
                trackpoint_docs.append(trackpoint_doc)
                self.tp_id += 1
        
        return trackpoint_docs

    def build_activity(self, user_id, activity_id, root, start_time: str, end_time:str):
        transportation_mode = None
        if user_id in self.labeled_users: # root is user with labels
            with open(root[:-10] + "labels.txt") as file:
                next(file)
                for label in file:
                    label_start = label.split()[0].replace("/", "-") + " " + label.split()[1]
                    label_end = label.split()[2].replace("/", "-") + " " + label.split()[3]
                    if label_start == start_time and label_end == end_time:
                        transportation_mode = label.split()[4]
                    

        return self.create_activity(activity_id, user_id, transportation_mode, start_time, end_time)
        

    def build_database(self):
        # self.create_collection("users") no need
        self.create_collection("activities")
        self.create_collection("trackpoints")
        self.init_users()
        trackpoint_docs = []; #empty array to fill with trackpoint documents
        activity_docs = []; #empty array to fill with activity documents

        activity_id = 0
        self.tp_id = 0
        for (root,dirs,files) in tqdm(os.walk("dataset/Data")):
            if root ==  "Data": # skips first iteration
                continue

            if "Trajectory" in root:
                user_id = root.split("/")[2] # sets the user id depending on folder

                for activity_file in files:
                    activity_file = root + "/" + activity_file
                    if(self.file_len(activity_file) >= 2506): # if activity has more than 2500 trackpoints -> skip
                        continue

                    # Trackpoints
                    trackpoints = self.build_trackpoints(activity_id, activity_file)
                    trackpoint_docs.extend(trackpoints)

                    #Activity
                    start_time = trackpoints[0]['date_time']
                    end_time = trackpoints[-1]['date_time']
                    activity_docs.append(self.build_activity(user_id, activity_id, root, start_time, end_time))        
                    activity_id += 1
        
        for key in trackpoint_docs[0]:
            print(type(key))
        self.insert_documents(trackpoint_docs, "trackpoints")
        self.insert_documents(activity_docs, "activities")


def main():
    program = None
    try:
        program = dbProgram()
        program.drop_all()
        program.build_database()
        program.show_collection()
    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    # this is the worst error handling as it doesnt give and info of where the error occured:( caused alot of frustration last assignment...
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
