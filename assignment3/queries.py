import datetime
from pprint import pprint
from re import match

from pymongo import collection
from pymongo.message import query
from DbConnector import DbConnector
import math
from tqdm import tqdm
from datetime import timedelta
from collections import Counter
from haversine import haversine


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
        if(hasattr(docs, '__iter__')):
            for doc in docs:
                pprint(doc)
        else:
            pprint(docs)

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
        docs = collection.aggregate([{
            '$group': {'_id': "$user_id"}
        }, {
            '$count': 'users'
        }])
        print("users:")
        self.print_documents(docs)

        docs = collection.find().count()
        print("activities:")
        self.print_documents(docs)

        collection = self.fetch_collection("trackpoints")
        docs = collection.find().count()
        print("trackpoints:")
        self.print_documents(docs)

    def task2(self):
        collection = self.fetch_collection("activities")
        docs = collection.aggregate([{
            '$group': {
                '_id': "$user_id",
                'activities': {
                    '$sum': 1
                }
            }
        }, {
            '$group': {
                '_id': None,
                'min': {
                    '$min': '$activities',
                },
                'max': {
                    '$max': '$activities',
                },
                'avg': {
                    '$avg': '$activities',
                }
            }
        }])
        print("min max and average number of activities:")
        self.print_documents(docs)

    def task3(self):
        collection = self.fetch_collection("activities")
        docs = collection.aggregate([{
            '$group': {
                '_id': "$user_id",
                'activities': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {'activities': -1}
        },
            {
            "$limit": 10
        }])
        print("most number of activities:")
        self.print_documents(docs)

    def task4(self):
        collection = self.fetch_collection("activities")
        docs = collection.aggregate([{
            '$project': {
                '_id': 1,
                'user_id': 1,
                'end_date': {
                    '$dateTrunc': {
                        'date': {
                            '$dateFromString': {
                                'dateString': '$end_date_time'
                            }
                        },
                        'unit': 'day'
                    }
                },
                'start_date': {
                    '$dateTrunc': {
                        'date': {
                            '$dateFromString': {
                                'dateString': '$start_date_time'
                            }
                        },
                        'unit': 'day'
                    }
                },
            }
        },
            {
            '$match': {
                '$expr': {
                    '$ne': [
                        '$start_date',
                        '$end_date'
                    ]
                }
            }

        }, {
            '$group': {
                '_id': "$user_id"
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }
        ])
        print("most number of activities:")
        self.print_documents(docs)

    def task5(self):
        collection = self.fetch_collection("activities")

        docs = collection.aggregate([
            {
                "$group": {
                    "_id": {"user_id": "$user_id", "transportation_mode": "$transportation_mode", "start_date_time": "$start_date_time", "end_date_time": "$end_date_time"},
                    "uniqueIds": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            }
        ])
        self.print_documents(docs)

    def task6(self):
        # An infected person has been at position (lat, lon) (39.97548, 116.33031) at
        # time ‘2008-08-24 15:38:00’. Find the user_id(s) which have been close to this
        # person in time and space (pandemic tracking). Close is defined as the same
        # minute (60 seconds) and space (100 meters). (This is a simplification of the
        # “unsolvable” problem given i exercise 2).
        pass

    def task7(self):
        # Find all users that have never taken a taxi.
        pass

    def task8(self):
        # Find all types of transportation modes and count how many distinct users that
        # have used the different transportation modes. Do not count the rows where the
        # transportation mode is null .
        pass

    def task9(self):
        # a)
        # Find the year and month with the most activities.

        # b)
        # Which user had the most activities this year and month, and how many
        # recorded hours do they have? Do they have more hours recorded than the user
        # with the second most activities?

        pass

    def task10(self):
        # Find the total distance (in km) walked in 2008, by user with id=112.
        
        def check_if_year_is_2008(activity):
            return datetime.datetime.strptime(activity["start_date_time"], '%Y-%m-%d %H:%M:%S').year == 2008

        total_km = 0
        activity_collection = self.fetch_collection("activities")

        user_112_activities = activity_collection.find({"user_id": "112"})
        user_112_activities_list = list(user_112_activities)
        #self.print_documents(user_112_activities_list)

        activities_in_2008 = list(filter(check_if_year_is_2008, user_112_activities_list))
        trackpoints_in_2008 = {}
        
        print("fetching trackpoints")
        #i = 0
        for activity in tqdm(activities_in_2008):
            #if i == 1:    # TESTING ON ONE ACTIVITY BECAUSE IT TAKES 23 MINUTES IF WE ARE TO RUN IT 
            #    break     # FOR EVERY TRACKPOINT IN EVERY ACTIVITY OF THIS USER IN 2008
            trackpoints_in_2008[activity["_id"]] = list(self.fetch_collection("trackpoints").find({"activity_id": activity["_id"]}))
            #i += 1

        print("summing length")
        for activity_id, trackpoints in trackpoints_in_2008.items():
            print(trackpoints[i])
            for i in range(len(trackpoints) - 1):
                lat1 = float(trackpoints[i]["pos"]["latitude"])
                lon1 = float(trackpoints[i]["pos"]["longitude"])
                pos1 = (lat1, lon1)
                lat2 = float(trackpoints[i+1]["pos"]["latitude"])
                lon2 = float(trackpoints[i+1]["pos"]["longitude"])
                pos2 = (lat2, lon2)
                total_km += haversine(pos1, pos2)
        print("User 112 has walked a total distance of: "+str(total_km) +" km")        

    def task11(self):
        # collection = self.fetch_collection("trackpoints")
        # docs = collection.aggregate([
        #     {
        #         "$unwind": "$pos"
        #     },
        #     {
        #         "$group": {
        #             "_id": {
        #                 "activity_id": "$activity_id",
        #                 "altitude": "$pos.altitude",
        #             }
        #         }
        #     },
        #     {
        #         "$match": {
        #             "pos.altitude": {"$ne": -777}
        #         }
        #     },           
        # ], allowDiskUse=True)
        # self.print_documents(docs)

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

        trackpoint_collection = self.fetch_collection("trackpoints")
        activity_collection = self.fetch_collection("activities")
        # trackpoints = trackpoint_collection.find({})

        # meters_gained = {}
        # prev_activity = None
        # prev_altitude = None
        # for trackpoint in tqdm(trackpoints, total=9676756):

        #     altitude = trackpoint["pos"]["altitude"]
        #     activity_id = trackpoint["activity_id"]

        #     if altitude == -777:
        #         continue

        #     if prev_altitude == None and prev_activity == None:
        #         prev_activity = activity_id
        #         prev_altitude = altitude

        #     activity = activity_collection.find({}).__getitem__(activity_id)
        #     user_id = activity["user_id"]

        #     if altitude > prev_altitude and activity_id == prev_activity:
        #         try:
        #             meters_gained[user_id] += altitude - prev_altitude
        #         except:
        #             meters_gained[user_id] = altitude - prev_altitude

        #     prev_altitude = altitude
        #     prev_activity = activity_id
           
            
        # top_users = Counter(meters_gained).most_common(20)
        # pprint(top_users)

        for idx in range(trackpoint_collection.count()):
            if idx == 0:
                continue

            test = trackpoint_collection.find({ "id": { "$lte": idx } }, {"id": 1, "pos.altitude": 1 }).sort({"_id": -1}).limit(2);
            print(test)


    def task12(self):
        # Find all users who have invalid activities, and the number of invalid activities per user
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
