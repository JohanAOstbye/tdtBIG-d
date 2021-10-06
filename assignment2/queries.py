from DbConnector import DbConnector
from tabulate import tabulate
import math

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
    
    def fetch_data(self, query, table_name):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    # the space [x,y,z] where the plane [x,y] is [latitude,longitude], and z is altitude.
    def calculateDistance3D(x1, x2, y1, y2, z1, z2):
        return math.sqrt(math.pow(x2 - x1) + math.pow(y2 - y1) + math.pow(z2 - z1))

    def isCloseInDistance(self, lat1, lat2, long1, long2, alt1, alt2):
        return self.calculateDistance3D(lat1, lat2, long1, long2, alt1, alt2) <= 100

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
        query = """
            SELECT user_id
            FROM User u
            WHERE EXISTS(
              SELECT trackpoint_id, activity_id, lat, long, altitude, date_time
              FROM Trackpoint t1
              WHERE EXISTS (
                SELECT user_id, activity_id, start_date_time, end_date_time
                FROM Activity a1
                INNER JOIN Activity a2
                  ON (a2.start_date_time > a1.start_date_time AND a2.start_date_time < a1.end_date_time) 
                  OR (a2.start_date_time > a1.start_date_time AND a2.end_date_time < a1.end_date_time) 
                  OR (a2.end_date_time < a1.end_date_time AND a2.end_date_time > a1.start_date_time) 
                WHERE t1.activity_id == a1.activity_id)
              INNER JOIN Trackpoint t2
                ON (isCloseInTime(calculateTimeBetween(t1.date_time, t2.date_time)))
                AND (isCloseInDistance(calculateDistance3D(t1.lat,t2.lat,t1.long,t2.long,t1.altitude,t2.altitude)))
        """
        print(query)

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

        query = """
        SELECT EXTRACT(month FROM start_date_time) "Month", EXTRACT(year FROM start_date_time) "Year", count(*) AS 'ant'
        FROM Activity
        GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        ORDER BY ant DESC, EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        """
        # GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        # ORDER BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        self.fetch_data(query,"Activity")

    def task10(self):
        query = """
        
        """
        print(query)

    def task11(self):
        query = """
        
        """
        print(query)

    def task12(self):
        query = """
        
        """
        print(query)

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
