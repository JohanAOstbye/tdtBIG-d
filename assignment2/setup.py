from DbConnector import DbConnector
from tabulate import tabulate
import os
from tqdm import tqdm

class dbProgram:
    
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, table_fields):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   %s
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % (table_name, table_fields))
        self.db_connection.commit()

    def insert_data(self, table_name, table_data):
        # inserts rows of data
        print("Inserting into %s" % table_name)
        for row in tqdm(table_data):
            query = "INSERT INTO %s VALUES (%s)"
            self.cursor.execute(query % (table_name, row))
        self.db_connection.commit()

    # def fetch_data(self, table_name):
    #     query = "SELECT * FROM %s"
    #     self.cursor.execute(query % table_name)
    #     rows = self.cursor.fetchall()
    #     print("Data from table %s, raw format:" % table_name)
    #     print(rows)
    #     # Using tabulate to show the table in a nice way
    #     print("Data from table %s, tabulated:" % table_name)
    #     print(tabulate(rows, headers=self.cursor.column_names))
    #     return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        #shows all tablenames
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def show_table(self, table_name):
        #shows a table with values
        print("Table %s" % table_name)
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def create_tables(self):
        #sets up database
        self.create_table(
            "User",
            """
            id VARCHAR(3) PRIMARY KEY,
            has_labels BIT(1)
            """
        )
        self.create_table(
            "Activity",
            """
            id INT NOT NULL PRIMARY KEY,
            user_id VARCHAR(3),
            transportation_mode CHAR(10),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
            """
        )
        self.create_table(
            "Trackpoint",
            """
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id)
            """
        )
    
    def drop_tables(self):
        #flushes db
        self.drop_table("Trackpoint")
        self.drop_table("Activity")
        self.drop_table("User")

    def insert_users(self):
        self.labeled_users = [] #list of user with labels on their activities
        with open("dataset/labeled_ids.txt") as file:
            for user in file:
                user = user[:-1] #remove /n
                self.labeled_users.append(user) #storing everything in memory!
        
        query_data = []

        for num in range(182): # formates users into querydata
            user = ("%03d" % (num,))
            has_label = "0"
            if user in self.labeled_users:
                has_label = "1"
            query_data.append("'" + user + "', " + has_label)
            

        self.insert_data(
            "User",
            query_data
        )

    def insert_activities_and_trackpoints(self):
        activity_id = 0
        activities = []
        trackpoints = []

        for (root,dirs,files) in tqdm(os.walk("dataset/Data")):
            if root ==  "Data": # skips first iteration
                continue

            if "Trajectory" in root:
                user_id = root.split("/")[2] # sets the user id depending on folder

                for activity_file in files:
                    activity_file_url = root + "/" + activity_file
                    if(self.file_len(activity_file_url) >= 2506): # if activity has more than 2500 trackpoints -> skip
                        continue

                    # Trackpoints
                    activity_trackpoints = []
                    with open(activity_file_url) as activity:
                        for x in range(6):
                            next(activity) # skips the first 6 lines of the .plt file

                        for trackpoint in activity:
                            lat = trackpoint.split(",")[0]
                            lon = trackpoint.split(",")[1]
                            altitude = trackpoint.split(",")[3]
                            date_time = trackpoint.split(",")[5] + " " + trackpoint.split(",")[6]
                            trackpoint = "%s, %s, %s, %s, %s" % (activity_id, lat, lon, altitude, date_time)
                            activity_trackpoints.append(trackpoint)
                    
                    trackpoints.extend(activity_trackpoints)

                    #Activity                    
                    transportation_mode = "NULL"
                    start_time = activity_trackpoints[0].split(",")[4][1:-1]
                    end_time = activity_trackpoints[-1].split(",")[4][1:-1]
                    if user_id in self.labeled_users: # root is user with labels
                        with open(root[:-10] + "labels.txt") as file:
                            next(file)
                            for label in file:
                                label_start = label.split()[0].replace("/", "-") + " " + label.split()[1]
                                label_end = label.split()[2].replace("/", "-") + " " + label.split()[3]
                                if label_start == start_time and label_end == end_time:
                                    transportation_mode = label.split()[4]
                                

                    activity = "%s, '%s', '%s', %s, %s" % (activity_id, user_id, transportation_mode, start_time, end_time)
                    activity_id += 1
                    activities.append(activity)
        
        print(activities[0])
        self.insert_data(
            "Activity",
            activities
        )
        self.insert_data(
            "Trackpoint",
            trackpoints
        )

    def file_len(self, file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1


def main():
    program = None
    try:
        program = dbProgram()
        
        program.create_tables()
        program.insert_users()

        program.insert_activities_and_trackpoints()
        # program.show_table("User")
        # program.show_table("Activity")
        # program.show_table("Trackpoint")

    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.drop_tables()
            program.connection.close_connection()


if __name__ == '__main__':
    main()

#YYYY-MM-DD HH:MM:SS