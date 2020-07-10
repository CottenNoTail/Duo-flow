import datetime
import os
import sys

# This is the interval between two heartbeats
HEARTBEAT_INTERVAL = 5


class Config:

    def get_schedulers(self):
        """ Returns the scheduler ip """
        main = os.environ['main']
        standby = os.environ['standby']
        SCHEDULER_List = list()
        SCHEDULER_List.append(main)
        SCHEDULER_List.append(standby)
        return SCHEDULER_List


    def get_heartbeat_interval(self):
        """ Returns the heartbeat interval in secs"""
        return HEARTBEAT_INTERVAL


    def get_airflow_home(self):
        """Returns the home directory of airflow."""
        if "AIRFLOW_HOME" in os.environ:
            return os.environ['AIRFLOW_HOME']
        else:
            return os.path.expanduser("~/airflow")        
        return AIRFLOW_HOME


    def get_start_command(self):
        return "nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &"

    def get_stop_command(self):
        return "cat airflow-scheduler.pid | xargs kill"

