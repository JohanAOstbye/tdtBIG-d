from DbConnector import DbConnector
from tabulate import tabulate
import math
from operator import itemgetter
from tqdm import tqdm

class Queries:
    
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
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
    
    def fetch_data(self, query, table_name, print_bool=True):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if(print_bool):
            # Using tabulate to show the table in a nice way
            print("Data from table %s, tabulated:" % table_name)
            print(tabulate(rows, headers=self.cursor.column_names, floatfmt=".0f"))
        return rows

    # the space [lat,lon,alt] where the plane [lat,lon] is [latitude,longitude], and alt is altitude.
    def calculateDistance3D(self, lat1, lat2, lon1, lon2, alt1, alt2):
        return math.sqrt(math.pow((lat2 - lat1),2) + math.pow((lon2 - lon1),2) + math.pow((alt2 - alt1),2))

    def isCloseInDistance(self, lat1, lat2, lon1, lon2, alt1, alt2):
        return self.calculateDistance3D(lat1, lat2, lon1, lon2, alt1, alt2) <= 100

    def calculateTimeBetween(datetime1, datetime2):
        date1 = datetime1.split()[0]
        date2 = datetime2.split()[0]
        if date2 != date1:
            return

        time1 = datetime1.split()[1]
        time2 = datetime2.split()[1]

        seconds1 = time1.split(":")[0]*3600 + \
            time1.split(":")[1] + time1.split(":")[0]
        seconds2 = time2.split(":")[0]*3600 + \
            time2.split(":")[1] + time2.split(":")[0]

        return seconds2 - seconds1

    def isCloseInTime(self, time1, time2, seconds):
        return self.calculateTime(time2, time1) <= seconds

    def dateFromDateTime(dateTime):
        # YYYY-MM-DD HH:MM:SS becomes YYYY-MM-DD
        return dateTime.split()[0]

    def task1(self):
        query_user = """
            SELECT COUNT(*) as "Number of Users"
            FROM User;
            """
            
        query_activity = """
            SELECT COUNT(*) as "Number of Activities"
            FROM Activity;
            """

        query_trackpoint = """
            SELECT COUNT(*) as "Number of Trackpoints"
            FROM Trackpoint;
            """
        self.fetch_data(query_user,"User")
        self.fetch_data(query_activity,"Activity")
        self.fetch_data(query_trackpoint,"Trackpoint")


    def task2(self):
        query = """
            SELECT COUNT(*) as NumberOfActivities, user_id
            FROM Activity
            GROUP BY (user_id)
            SELECT AVG(NumberOfActivities), MAX(NumberOfActivities), Min(NumberOfActivities);
            """
        self.fetch_data(query, "Activity")

    def task3(self):
        query = """
            SELECT COUNT(NumberOfActivities), user_id 
            FROM Activity
            ORDER BY COUNT(NumberOfActivities) DESC
            LIMIT 10;
            """
        self.fetch_data(query, "Activity")

    def task4(self):
        query = """
            SELECT user_id, 
            dateFromDateTime(start_date_Time) as start_date, 
            dateFromDateTime(end_date_Time) as end_date
            COUNT(IF(start_date != end_date))
            FROM Activities
            GROUP BY user_id
            """
        self.fetch_data(query, "Activity")

    def task5(self):
        query = """
            SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*)
            FROM Activity
            GROUP BY user_id, transportation_mode, start_date_time, end_date_time
            HAVING COUNT(*)>1; 
            """
        self.fetch_data(query, "Activity")

    def task6(self):
        queryTrackpointsWhereActivityOverlap = """
            SELECT trackpoint_id, activity_id, lat, long, altitude, date_time
            FROM Trackpoint t1
            WHERE EXISTS (
              SELECT user_id, activity_id, start_date_time, end_date_time
              FROM Activity a1
              INNER JOIN Activity a2
                ON (a2.start_date_time >= a1.start_date_time AND a2.start_date_time =< a1.end_date_time) 
                OR (a2.start_date_time >= a1.start_date_time AND a2.end_date_time =< a1.end_date_time) 
                OR (a2.end_date_time =< a1.end_date_time AND a2.end_date_time >= a1.start_date_time) 
              WHERE t1.activity_id == a1.activity_id)
        """

        queryActivityOverlap = """
            SELECT user_id, activity_id, start_date_time, end_date_time
            FROM Activity a1
            INNER JOIN Activity a2
              ON (a2.start_date_time >= a1.start_date_time AND a2.start_date_time =< a1.end_date_time) 
              OR (a2.start_date_time >= a1.start_date_time AND a2.end_date_time =< a1.end_date_time) 
              OR (a2.end_date_time =< a1.end_date_time AND a2.end_date_time >= a1.start_date_time) 
            ORDER BY start_date_time ASC
        """

        activityOverlap = self.fetch_data(queryActivityOverlap, "Activity")

        queryRelevantOverlap = """
            SELECT lat, long, altitude, date_time
            FROM Trackpoint t
            WHERE date_time >= %s AND date_time <= %s
        """

        for a in activityOverlap:
          relevantTrackpoints = self.fetch_data(queryRelevantOverlap % (a.start_date_time, a.end_date_time) , "Trackpoints")
          


        #for t1 in trackpointsWhereActivityOverlap:
        #  for t2 in trackpointsWhereActivityOverlap:
        #    if self.isCloseInTime(self.calculateTimeBetween(t1.date_time, t2.date_time)) and self.isCloseInDistance(self.calculateDistance3D(t1.lat,t2.lat,t1.long,t2.long,t1.altitude,t2.altitude) and t1 != t2):
        #      trackpointsCloseToOthers.append(t1)
        
        """
        INNER JOIN Trackpoint t2
              ON (isCloseInTime(calculateTimeBetween(t1.date_time, t2.date_time)))
              AND (isCloseInDistance(calculateDistance3D(t1.lat,t2.lat,t1.long,t2.long,t1.altitude,t2.altitude)))
        """

    def task7(self):
        query = """
        SELECT COUNT(id)
        FROM User
        WHERE id NOT IN (SELECT DISTINCT user_id as id FROM Activity WHERE transportation_mode = 'taxi')
        """
        self.fetch_data(query,"Activity")

    def task8(self):
        query = """
        SELECT transportation_mode,
            COUNT(*) AS 'ant'
        FROM Activity
        WHERE transportation_mode != 'NULL'
        GROUP BY transportation_mode
        """
        self.fetch_data(query,"Activity")

    def task9(self):
        # a)
        query = """
        SELECT EXTRACT(month FROM start_date_time) "Month", EXTRACT(year FROM start_date_time) "Year", count(*) AS 'ant'
        FROM Activity
        GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        ORDER BY ant DESC, EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        """
        # GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        # ORDER BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        self.fetch_data(query,"Activity")

        # b)
        query = """
        SELECT user_id, count(*) AS 'ant'
        FROM Activity
        WHERE EXTRACT(month FROM start_date_time) = 11 AND EXTRACT(year FROM start_date_time) = 2008
        GROUP BY user_id
        ORDER BY ant DESC, user_id;
        """
        # GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        # ORDER BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        self.fetch_data(query,"Activity")

    def task10(self):
        query = """
        SELECT Activity.id, Trackpoint.lat, Trackpoint.lon, Trackpoint.altitude
        FROM Trackpoint
        JOIN Activity ON Trackpoint.activity_id=Activity.id
        WHERE EXTRACT(year FROM start_date_time) = 2008 AND user_id = 112
        """
        rows = self.fetch_data(query,"Tracpoint and Activity", False)

        distance = 0
        last_trackpoint = rows[0]
        activity_id = 0

        for trackpoint in rows:
            if (trackpoint[0] != activity_id):
                activity_id = trackpoint[0]
                last_trackpoint = trackpoint
                continue
            
            if(trackpoint == last_trackpoint):
                continue #skips when the user didnt move and the first pos

            distance += self.calculateDistance3D(last_trackpoint[1], trackpoint[1], last_trackpoint[2], trackpoint[2], last_trackpoint[3], trackpoint[3])
        
        distance = distance / 1000
        print("Total distance: " + f'{distance:.1f}' + "km")

    def task11(self):
        query = """
        SELECT summed.id AS user_id, SUM(summed.sum_diff) AS total_meters_gained
        FROM (SELECT Activity.user_id AS id, difference_purged.activity_id, SUM(difference_purged.diff) AS sum_diff
            FROM Activity RIGHT JOIN (SELECT activity_id, diff
                FROM (SELECT activity_id, altitude, last_altitude, (altitude-last_altitude) AS diff
                    FROM (SELECT activity_id, altitude, LAG(altitude) OVER (PARTITION BY activity_id ORDER BY id) AS last_altitude
                        FROM Trackpoint
                        WHERE altitude != -777) AS altitudes
                    ) AS difference  
                WHERE diff > 0) AS difference_purged 
            ON Activity.id = difference_purged.activity_id  
            GROUP BY difference_purged.activity_id) AS summed
        GROUP BY user_id
        ORDER BY total_meters_gained DESC
        LIMIT 20
        """
        self.fetch_data(query, "Activity AND Trackpoint")         

    def task12(self):
        query = """
        SELECT activity_id, maxdiff, user_id
        FROM (
            SELECT activity_id, MAX(diff) AS 'maxdiff'
            FROM (
                SELECT activity_id, date_time, last_date_time, TIMESTAMPDIFF(SECOND, date_time, date_time) AS 'diff'
                FROM (
                    SELECT activity_id, date_time, LAG(date_time) OVER (PARTITION BY activity_id ORDER BY id) AS last_date_time FROM Trackpoint
                    ) AS times
                ) AS time_diff
            GROUP BY activity_id
            ) AS Max_diff
        JOIN Activity on Activity.id = Max_diff.activity_id
        """

        query = """
        SELECT activity_id, MAX(diff) AS 'maxdiff'
        FROM (
            SELECT activity_id, date_time, last_date_time, TIMESTAMPDIFF(SECOND, date_time, date_time) AS 'diff'
            FROM (
                SELECT activity_id, date_time, LAG(date_time) OVER (PARTITION BY activity_id ORDER BY id) AS last_date_time FROM Trackpoint
                ) AS times
            ) AS time_diff

        WHERE activity_id < 30
        GROUP BY activity_id
        """
        self.fetch_data(query,"Trackpoint")

    def tasks(self):
        return self.query_tasks
        


def main():
    program = None
    try:
        program = Queries()
        tasks = Queries.tasks(program)

        task = "start"

        while(True):
            task = input("Run task: ")

            if(task == "exit" or task == ""):
                break


            if (int(task) in tasks):
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
