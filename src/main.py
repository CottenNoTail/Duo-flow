import os
from datetime import datetime
import subprocess
import datetime
import time
from config import Config 
from db import 
from pytz import timezone

config = Config()
db = db()

class Duo_flow:
    def __init__(self):
        self.start_command = config.get_start_command()
        self.stop_command = config.get_stop_command()
        self.heartbeat_frequency = config.get_heartbeat_frequency()
        print ("------------Duo-flow is on ---------")


    def poll(self):
        """ Then main scheduler keeps updating last heartbeat time in database. The standby scheduler reads it and compare it with current time. 
             If time interval is larger than 15 seconds, the standby scheduler will take over the job."""
        
        last_heartbeat = db.get_heartbeat()
        eastern = timezone('US/Eastern')
        current_time = datetime.datetime.now(eastern)


        """ Then main scheduler and standby scheduler has different work branch"""
        ip_check_command = "hostname -i"
        output = os.popen(ip_check_command).read()
        scheduler_ip = config.get_scheduler()
        
        """ Then main scheduler updates the heartbeat"""
        if output + '\n'== scheduler_ip[0]:
            interval = (current_time - last_heartbeat).total_seconds()
            print("This is the main airflow scheduler, hearbeat interval:", round(interval,1))
            db.set_heartbeat()

        """ Then standby scheduler reads the heartbeat"""
        else:
            interval = (current_time - last_heartbeat).total_seconds()
            print("This is the standby scheduler, hearbeat interval:",round(interval,1))


            if interval > 15:
                os.popen(self.start_command)
                print("standby scheduler is on")
                db.init_heartbeat()
                db.set_heartbeat()
                db.set_scheduler("standby")

            else: 
           """ After standby scheduler takes over the job, it will also update the heartbeat"""
                on_scheduler = db.get_scheduler()
                if on_scheduler == "standby":
                    db.set_heartbeat()
        
 
def main():
    """ The main program of Duo-flow """
    df = Duo_flow()
    db.set_heartbeat()
    
    
    """ check if this machine is the main airflow scheduler, if yes, register in database """
    ip_check_command = "hostname -i"
    ip = os.popen(ip_check_command).read()
    if ip == scheduler_ip[0]:
        os.popen("nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &")        
    db.set_scheduler("airflow")


    """ Loop indefinitely and communicate with the other scheduler """
    while 1:
        ft.poll()
        print ("------------ Finish beating. Sleeping for 5 seconds------")
        time.sleep(config.get_heartbeat_frequency())



if __name__ == "__main__":
    main()
