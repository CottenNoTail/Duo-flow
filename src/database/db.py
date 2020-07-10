import os
import psycopg2
from datetime import datetime
from pytz import timezone
import datetime
from config import Config

config = Config()

class db:
 
    def connect_to_db(self):
        try:
            self.conn = psycopg2.connect(
                host=os.environ.get('HOST'),
                database='airflow9',
                user=os.environ.get('USER'),
                password=os.environ.get('PSWD')
            )
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close_conn(self):
        try:
            self.conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        
        
        
    def init_heartbeat(self):
        """ initialize heartbeat to current time """
        try:
            self.connect_to_db()
            cur = self.conn.cursor()
            cur.execute("""DELETE FROM Duo_flow WHERE key = %s;""", ('heartbeat', ))

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        cur.close()
        self.close_conn()
        
        
    def get_scheduler(self):
        """ Returns the working scheduler """
        try:
            self.connect_to_db()
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM Duo_flow WHERE key = %s;""", ('scheduler', ))
            row = cur.fetchall()


            """ Returns the working scheduler if there is one """
            if len(row) is not 0:               
                print("The running scheduler is",str(row[0][0]))     
                return row[0][0]  
            else:
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        cur.close()
        self.close_conn()
        
    def set_scheduler(self,flag):
        """ Checks if there is a scheduler. If not, it inserts a new one.
        If the scheduler already set, update it"""

        try:
            self.connect_to_db()
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM Duo_flow WHERE key = %s;""", ('scheduler',))
            row = cur.fetchall()

            # If the scheduler is there, update it
            if len(row) is not 0:
                update_statement = """UPDATE Duo_flow SET value = %s WHERE key = %s;"""
                cur.execute(update_statement, (flag,"scheduler"))
                print("The sheduler is set as " + str(flag))

            # If the scheduler doesn't exist, insert it
            else:
                insert_statement = """INSERT INTO Duo_flow VALUES (%s, %s) RETURNING value;"""
                cur = self.conn.cursor()
                cur.execute(insert_statement, ("scheduler", flag))
                return_value = cur.fetchone()[0]
                print("The sheduler is set as " + str(flag))

            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        cur.close()
        self.close_conn()
        

    def get_heartbeat(self):
        """ Returns the latest heartbeat time """
        try:
            self.connect_to_db()
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM Duo_flow WHERE key = %s;""", ('heartbeat', ))
            row = cur.fetchall()

            # If there is a time for the last heartbeat, return it

            if len(row) is not 0:
                eastern = timezone('US/Eastern')
                return eastern.localize(config.get_string_as_datetime(row[0][0]))

        # If there is no time for the heartbeat, return none
            else:
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        cur.close()
        self.close_conn()


    def set_heartbeat(self):
        """ Checks if there is a heartbeat. If not, it inserts a new one.
        If the heartbeat already set, update it"""

        try:
            self.connect_to_db()
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('heartbeat',))
            row = cur.fetchall()


            eastern = timezone('US/Eastern')
            heartbeat_datetime = config.get_datetime_as_str(datetime.datetime.now(eastern))

            if len(row) is not 0:
                update_statement = """UPDATE Duo_flow SET value = %s WHERE key = %s;"""
                cur = self.conn.cursor()
                cur.execute(update_statement, (heartbeat_datetime, "heartbeat"))

            else:
                insert_statement = """INSERT INTO Duo_flow VALUES (%s, %s) RETURNING value;"""
                cur = self.conn.cursor()
                cur.execute(insert_statement, ("heartbeat", heartbeat_datetime))

            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        cur.close()
        self.close_conn()


