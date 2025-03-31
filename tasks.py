
import os
import time
import logging
import win32serviceutil
import win32event
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.background import BackgroundScheduler

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from pymongo import MongoClient
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import config

#logging-setup --> start

logging.basicConfig(level=logging.INFO)

log_dir = "C:\\StreamSyncX\\logs"
log_file = os.path.join(log_dir, "tasks_debug.log")
os.makedirs(log_dir, exist_ok=True)

log_handler = RotatingFileHandler(
    log_file, maxBytes=1* 1024 * 1024, backupCount=3, encoding="utf-8"
)
log_handler.setLevel(logging.INFO)

log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

logging.getLogger().addHandler(log_handler)

#logginf setup --> end

logging.info("StreamSyncXService PSG Data fetch and Delete Tasks Service: Initialization started.")
class StreamSyncXService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PSGStreamSyncXService"
    _svc_display_name_ = "PSG EDS Data Fetching"
    _svc_description_ = "A Windows Service that fetches XML data and stores it in MongoDB using Dramatiq and Redis."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

        self.broker = RedisBroker(host="localhost", port=6379)
        dramatiq.set_broker(self.broker)

        self.client = MongoClient("mongodb://smartgrid:yQPJi5bLVrWLsd6s@federateddatabaseinstance0-yuuy7.a.query.mongodb.net/?ssl=true&authSource=admin&appName=FederatedDatabaseInstance0")
        self.db = self.client["VirtualDatabase0"]
        self.collection = self.db["PSGData"]

        self.scheduler = BackgroundScheduler()
        logging.info("Job Scheduler Started Successfully.")
    
    def SvcStop(self):
        logging.info("Service is shutting down...")
        self.running = False
        self.scheduler.shutdown(wait=False)
        win32event.SetEvent(self.stop_event)
        logging.info("Service stopped successfully.")

    def SvcDoRun(self):
        logging.info("Service is now running.")
        try:
            if not self.scheduler.running:
                self.scheduler.add_job(self.fetch_data.send, "interval", seconds=10)
                self.scheduler.add_job(self.delete_old_data.send, "interval", seconds=60)
                self.scheduler.start()
                logging.info("Scheduler started.")
            else:
                logging.info("Scheduler is already running.")
        except Exception as e:
            logging.error(f"Scheduler startup failed: {e}")
        
        while self.running:
            try:
                logging.info("Service is active and running.")
                time.sleep(10)
            except Exception as e:
                logging.error(f"Error encountered in service loop: {e}")

    @staticmethod
    @dramatiq.actor
    def fetch_data():
        client = MongoClient("mongodb://smartgrid:yQPJi5bLVrWLsd6s@federateddatabaseinstance0-yuuy7.a.query.mongodb.net/?ssl=true&authSource=admin&appName=FederatedDatabaseInstance0")
        db = client["VirtualDatabase0"]
        collection = db["PSGData"]

        try:

            now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
            dateTime_str = now_ist.strftime("%Y-%m-%d %H:%M:%S")
            date_str = now_ist.date().strftime("%Y-%m-%d")
            time_str = now_ist.time().strftime("%H:%M:%S")

            for url in config.urls:
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        root = ET.fromstring(response.content)
                        variable_elements = root.findall(".//variable")

                        data_records = []
                        for variable_element in variable_elements:
                            try:
                                d_name = variable_element.find("id").text
                                d_value = float(variable_element.find("value").text)
                                data_records.append({
                                    "id": d_name,
                                    "value": d_value,
                                    "date_time": dateTime_str,
                                    "date": date_str,
                                    "time": time_str,
                                    "datetime_obj": now_ist
                                })
                            except (AttributeError, ValueError):
                                continue

                        if data_records:
                            collection.insert_many(data_records)
                            logging.info(f"Successfully stored {len(data_records)} records from {url}.")
                    else:
                        logging.error(f"Data fetch failed from {url} - Status code: {response.status_code}")
                except Exception as e:
                    logging.error(f"Exception while fetching data from {url}: {e}")
        except Exception as e:
            logging.error(f"Error while reading config.py file: {e}")

    @staticmethod
    @dramatiq.actor
    def delete_old_data():
        client = MongoClient("mongodb://smartgrid:yQPJi5bLVrWLsd6s@federateddatabaseinstance0-yuuy7.a.query.mongodb.net/?ssl=true&authSource=admin&appName=FederatedDatabaseInstance0")
        db = client["VirtualDatabase0"]
        collection = db["PSGData"]
        try:
            now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
            expiration_time = now_ist - timedelta(minutes=5)

            result = collection.delete_many({"datetime_obj" : {"$lte" : expiration_time}})
            logging.info(f"Deleted {result.deleted_count} old records from MongoDB.")
        except Exception as e:
            logging.error(f"Error while deleting expired data: {e}")

if __name__ =="__main__":
    win32serviceutil.HandleCommandLine(StreamSyncXService)
