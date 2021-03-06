from DbConnector import DbConnector
from tabulate import tabulate
import math
from operator import itemgetter
from tqdm import tqdm
from datetime import timedelta

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

    def calculateTimeBetween(self, datetime1, datetime2):
      """
      print(datetime1)
      print(datetime2)

      date1 = datetime1.date
      date2 = datetime2.date
      
      print(date1)
      print(date2)
      if date2 != date1: return 999
      
      hour1 = datetime1.hour
      hour2 = datetime2.hour
      min1 = datetime1.minute
      min2 = datetime2.minute
      sec1 = datetime1.second
      sec2 = datetime2.second

      seconds1 = hour1*3600 + min1*60 + sec1
      seconds2 = hour2*3600 + min2*60 + sec2"""
      return (datetime1 - datetime2).total_seconds()

    def isCloseInTime(self, time1, time2, seconds):
        return self.calculateTimeBetween(time2, time1) <= seconds

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
            SELECT MAX(NumberOfActivities) AS maximum, MIN(NumberOfActivities) AS minimum, AVG(NumberOfActivities) AS average
            FROM (SELECT COUNT(*) as NumberOfActivities, user_id
                FROM Activity
                GROUP BY user_id) AS num
            """
        self.fetch_data(query, "Activity")

    def task3(self):
        query = """
            SELECT user_id, NumberOfActivities
            FROM (SELECT COUNT(*) as NumberOfActivities, user_id
                FROM Activity
                GROUP BY user_id) AS num
            ORDER BY NumberOfActivities DESC
            LIMIT 10;
            """
        self.fetch_data(query, "Activity")

    def task4(self):
        query = """
            SELECT COUNT(*) AS amount
            FROM (SELECT user_id, COUNT(*)
                FROM (SELECT user_id, DATE(start_date_time) as start_date, DATE(end_date_time) as end_date
                    FROM Activity) AS dating
                WHERE start_date != end_date
                GROUP BY user_id) AS why
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
        SELECT DISTINCT
            st.id,
            a1.user_id,
            a2.user_id
        FROM
            Trackpoint tp1,
            Trackpoint tp2,
            Activity a1,
            Activity a2,
            (SELECT
                (@row_number:=@row_number + 1) AS id,
                a1.id as act_id_1,
                a2.id as act_id_2
            FROM
                Activity a1
            INNER JOIN
                Activity a2
            ON
                a1.id <> a2.id
            WHERE   
                (
                a2.start_date_time BETWEEN date_add(a1.start_date_time, interval -1 minute) AND date_add(a1.end_date_time, interval 1 minute) OR
                a2.end_date_time BETWEEN date_add(a1.start_date_time, interval -1 minute) AND date_add(a1.end_date_time, interval 1 minute)
                ) AND
                a1.user_id <> a2.user_id
            GROUP BY
                a1.id, a2.id) as st
        WHERE
            a1.id = tp1.activity_id AND
            a2.id = tp2.activity_id AND
            a1.user_id <> a2.user_id AND
            tp1.activity_id = st.act_id_1 AND
            tp2.activity_id = st.act_id_2 AND
            SQRT(POWER((tp2.lat - tp1.lat),2) + POWER((tp2.lon - tp1.lon),2) + POWER((tp2.altitude - tp1.altitude),2)) < 0.001 AND
            (TIMESTAMPDIFF(SECOND, tp1.date_time, tp2.date_time) < 60 AND TIMESTAMPDIFF(SECOND, tp1.date_time, tp2.date_time) > -60);
        """
      self.fetch_data(query, "Activity")

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
            COUNT(*) AS 'amount'
        FROM Activity
        WHERE transportation_mode != 'NULL'
        GROUP BY transportation_mode
        """
        self.fetch_data(query,"Activity")

    def task9(self):
        # a)
        query = """
        SELECT EXTRACT(month FROM start_date_time) "Month", EXTRACT(year FROM start_date_time) "Year", count(*) AS 'amount'
        FROM Activity
        GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        ORDER BY amount DESC, EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        LIMIT 1
        """
        # GROUP BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time)
        # ORDER BY EXTRACT(year FROM start_date_time), EXTRACT(month FROM start_date_time);
        self.fetch_data(query,"Activity")

        # b)
        query = """
        SELECT user_id, SUM(hours) as 'hours', COUNT(*) AS 'Activities'
        FROM (SELECT user_id, (TIMESTAMPDIFF(SECOND, start_date_time, end_date_time)/3600) AS 'hours'
            FROM Activity
            WHERE month(start_date_time) = 11 AND year(start_date_time) = 2008
        ) AS all_hours
        GROUP BY user_id
        ORDER BY activities DESC, user_id
        LIMIT 2
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
        SELECT user, total_invalid as 'total invalid'
        FROM (
            SELECT user_id AS user, COUNT(*) AS 'total_invalid'
            FROM (
                SELECT activity_id, MAX(diff) AS 'maxdiff'
                FROM (
                    SELECT activity_id, date_time, last_date_time, TIMESTAMPDIFF(MINUTE, last_date_time, date_time) AS 'diff'
                    FROM (
                        SELECT activity_id, date_time, LAG(date_time) OVER (PARTITION BY activity_id) AS last_date_time FROM Trackpoint
                        ) AS times
                    ) AS time_diff
                GROUP BY activity_id
                ) AS Max_diff
            LEFT JOIN Activity on Activity.id = Max_diff.activity_id
            WHERE maxdiff > 300
            GROUP BY user_id
        ) AS invalids
        ORDER BY total_invalid DESC
        """

        # RIGHT JOIN Activity on Activity.id = Max_diff.activity_id
        # query = """
        # SELECT activity_id, MIN(diff) AS 'maxdiff'
        # FROM (
        #     SELECT activity_id, date_time, last_date_time, TIMESTAMPDIFF(SECOND, last_date_time, date_time) AS 'diff'
        #     FROM (
        #         SELECT activity_id, date_time, LAG(date_time) OVER (PARTITION BY activity_id ORDER BY id) AS last_date_time FROM Trackpoint
        #         ) AS times
        #     ) AS time_diff
        # GROUP BY activity_id
        # """
        rows = self.fetch_data(query,"Trackpoint")
        print(len(rows))

    def tasks(self):
        return self.query_tasks
        


def main():
    program = None
    try:
        program = Queries()

        #print(program.calculateTimeBetween('2007-08-02 15:46:14', '2007-08-02 15:46:00'))
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


