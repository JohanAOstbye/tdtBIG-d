import setup
import math


class Queries:

    # the space [x,y,z] where the plane [x,y] is [latitude,longitude], and z is altitude.
    def calculateDistance3D(x1, x2, y1, y2, z1, z2):
        return math.sqrt(math.pow(x2 - x1) + math.pow(y2 - y1) + math.pow(z2 - z1))

    def isCloseInDistance(self, lat1, lat2, long1, long2, alt1, alt2):
        return self.calculateDistance3D(lat1, lat2, long1, long2, alt1, alt2) <= 100

    def calculateTime(datetime1, datetime2):
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
        query = """
        SELECT COUNT(*) as "Number of Users"
        FROM Users;

        SELECT COUNT(*) as "Number of Activities"
        FROM Activities;

        SELECT COUNT(*) as "Number of Trackpoints"
        FROM Trackpoints;
        """
        print(query)

    def task2(self):
        query = """
        SELECT COUNT(*) as NumberOfActivities, user_id
        FROM Activities

        GROUP BY (user_id)

        SELECT AVG(NumberOfActivities), MAX(NumberOfActivities), Min(NumberOfActivities);
        """
        print(query)

    def task3(self):
        query = """
        SELECT COUNT(NumberOfActivities), user_id 
        FROM Activities
        ORDER BY COUNT(NumberOfActivities) DESC
        LIMIT 10;
        """
        print(query)

    def task4(self):
        query = """
        SELECT user_id, 
        dateFromDateTime(start_date_Time) as start_date, 
        dateFromDateTime(end_date_Time) as end_date
        COUNT(IF(start_date != end_date))
        FROM Activities
        GROUP BY user_id
        """
        print(query)

    def task5(self):
        query = """
        
        """
        print(query)

    def task6(self):
        query = """
        
        """
        print(query)

    def task7(self):
        query = """
        
        """
        print(query)

    def task8(self):
        query = """
        
        """
        print(query)

    def execute_query(self, task):
        tasks = {
            1: self.task1
        }
        tasks[task]()


def main():
    program = None
    try:
        program = setup.dbProgram()

    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
