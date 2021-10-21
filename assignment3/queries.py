from pprint import pprint

from pymongo import collection
from DbConnector import DbConnector
import math
from tqdm import tqdm
from datetime import timedelta


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.query_tasks = {
            1: self.task1,
            2: self.task2,
            3: self.task3,
            4: self.task4,
            5: self.task5,
            6: self.task6,
            7: self.task7,
            8: self.task8,
            9: self.task9,
            10: self.task10,
            11: self.task11,
            12: self.task12,
        }

    def fetch_collection(self, collection_name):
        return self.db[collection_name]

    def print_documents(self, docs):
        for doc in docs:
            pprint(doc)

    # the space [lat,lon,alt] where the plane [lat,lon] is [latitude,longitude], and alt is altitude.
    # def calculateDistance3D(self, lat1, lat2, lon1, lon2, alt1, alt2):
    #     return math.sqrt(math.pow((lat2 - lat1),2) + math.pow((lon2 - lon1),2) + math.pow((alt2 - alt1),2))

    # def isCloseInDistance(self, lat1, lat2, lon1, lon2, alt1, alt2):
    #     return self.calculateDistance3D(lat1, lat2, lon1, lon2, alt1, alt2) <= 100

    # def calculateTimeBetween(self, datetime1, datetime2):
    #   """
    #   print(datetime1)
    #   print(datetime2)

    #   date1 = datetime1.date
    #   date2 = datetime2.date

    #   print(date1)
    #   print(date2)
    #   if date2 != date1: return 999

    #   hour1 = datetime1.hour
    #   hour2 = datetime2.hour
    #   min1 = datetime1.minute
    #   min2 = datetime2.minute
    #   sec1 = datetime1.second
    #   sec2 = datetime2.second

    #   seconds1 = hour1*3600 + min1*60 + sec1
    #   seconds2 = hour2*3600 + min2*60 + sec2"""
    #   return (datetime1 - datetime2).total_seconds()

    # def isCloseInTime(self, time1, time2, seconds):
    #     return self.calculateTimeBetween(time2, time1) <= seconds

    # def dateFromDateTime(dateTime):
    #     # YYYY-MM-DD HH:MM:SS becomes YYYY-MM-DD
    #     return dateTime.split()[0]

    def task1(self):
        collection = self.fetch_collection("activities")
        query = {}
        docs = collection.find(query)  # .sort().limit()
        self.print_documents(docs)

    def task2(self):
        pass

    def task3(self):
        pass

    def task4(self):
        pass

    def task5(self):
        collection = self.fetch_collection("activities")
        
        docs = collection.aggregate([
            { 
                "$group": { 
                    "_id": { "user_id": "$user_id", "transportation_mode": "$transportation_mode", "start_date_time": "$start_date_time", "end_date_time": "$end_date_time" }, 
                    "uniqueIds": { "$addToSet": "$_id" },
                    "count": { "$sum": 1 } 
                }
            }, 
            { 
                "$match": {
                    "count": { "$gt": 1 } 
                } 
            }
        ])
        self.print_documents(docs)

    def task6(self):
        pass

    def task7(self):
        pass

    def task8(self):
        pass

    def task9(self):
        # a)

        # b)
        pass

    def task10(self):
        pass

    def task11(self):
        # collection = self.fetch_collection("trackpoints")
        # docs = collection.aggregate([
        #     {
        #         "$group": {
        #             "_id": 0
        #         }
        #     },
        #     {
        #         "$lookup": {
        #             "from": "activities",
        #             "let": {},
        #             "pipeline": [
        #                 { "$group": {
        #                     "_id": {"user_id": "$user_id"}
        #                 }}
        #             ],
        #             "as": "activities"
        #         }
        #     },
        #     {
        #         "$lookup": {
        #             "from": "trackpoints",
        #             "let": {},
        #             "pipeline": [
        #                 # {
        #                 #     "$unwind": {"$pos"}
        #                 # },
        #                 { 
        #                     "$group": {
        #                         "_id": {"activity_id": "$activity_id", "altitude": "$pos.altitude"}
        #                     }
        #                 }
        #             ],
        #             "as": "trackpoints"
        #         }
        #     },
        #         {
        #         "$unwind": {
        #             "path" : '$activities',
        #         }
        #     },
        #     {
        #         "$unwind": {
        #             "path" : '$trackpoints',
        #         }
        #     },
        #     {
        #         "$project": {
        #             "user_id": "$activities.user_id",
        #             "activity_id": "$trackpoints.activity_id",
        #             "altitude": "$trackpoints.altitude"
        #         }
        #     },
        #     { "$limit": 5 }
        # ])
        # self.print_documents(docs)
        # print(type(docs))

        # sums = {}
        # prev_altitude = None
        # prev_activity = None
        # for doc in docs:
        #     if doc.altitude == -777:
        #         continue

        #     if prev_altitude == None and prev_activity == None:
        #         prev_altitude = doc.altitude
        #         prev_activity = doc.activity

        #     if doc.altitude > prev_altitude and doc.activity == prev_activity:
        #         sums[doc.user_id] += doc.altitude - prev_altitude

        # print(sums) 

        activities = self.fetch_collection("activities")
        trackpoints = self.fetch_collection("trackpoints")

        

    def task12(self):
        pass

    def tasks(self):
        return self.query_tasks


def main():
    program = None
    try:
        program = Queries()
        tasks = Queries.tasks(program)
        task = ""

        while(True):
            task = input("Run task: ")

            if(task == "exit" or task == ""):
                break

            if task.isdigit() and int(task) in tasks:
                tasks[int(task)]()
            else:
                print("task %s not valid: " % task)

    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
