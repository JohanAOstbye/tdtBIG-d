from DbConnector import DbConnector
from tabulate import tabulate

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
        for row in table_data:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s VALUES ('%s')"
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
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def show_table(self, table_name):
        print("Table %s" % table_name)
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def file_len(self, file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

def main():
    program = None
    try:
        program = dbProgram()
        program.create_table(
            "User",
            """
            id VARCHAR(3) PRIMARY KEY,
            has_labels BIT(1)
            """
        )
        program.create_table(
            "Activity",
            """
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(3),
            transportation_mode CHAR(10),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
            """
        )
        program.create_table(
            "TrackPoint",
            """
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id)
            """
        )
        program.show_table("User")
        program.show_table("Activity")
        program.show_table("TrackPoint")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.drop_table("TrackPoint")
            program.drop_table("Activity")
            program.drop_table("User")
            program.connection.close_connection()


if __name__ == '__main__':
    main()
