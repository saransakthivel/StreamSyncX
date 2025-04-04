
import os
import psutil
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

if not logging.getLogger().hasHandlers():
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
    _svc_name_ = "PsgStreamSyncXService"
    _svc_display_name_ = "PSG EDS Data Fetching"
    _svc_description_ = "A Windows Service that fetches XML data and stores it in MongoDB using Dramatiq and Redis."

    _MONGO_URL = "mongodb+srv://smartgrid:yQPJi5bLVrWLsd6s@psgsynccluster.yuuy7.mongodb.net/"
    _DB_NAME = "test_db"
    _COLLECTION_NAME = "test_collection"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

        self.broker = RedisBroker(host="localhost", port=6379)
        dramatiq.set_broker(self.broker)

        self.client = MongoClient(self.__class__._MONGO_URL)
        self.db = self.client[self.__class__._DB_NAME]
        self.collection = self.db[self.__class__._COLLECTION_NAME]

        self.scheduler = BackgroundScheduler()
        logging.info("Job Scheduler Started Successfully.")
    
    def SvcStop(self):
        logging.info("Service is shutting down...")
        self.running = False

        try:
            self.scheduler.shutdown(wait=False)
        except Exception as e:
            logging.error(f"Error Shutting down service: {e}")

        try:
            parent = psutil.Process(os.getpid())
            logging.info(f"Terminating Process Parent:{os.getpid()}")

            children = parent.children(recursive=True)
            logging.info(f"Found {len(children)} child process(es).")

            if not children:
                logging.info("No child process found.")
            else:
                for child in children:
                    try:
                        logging.info(f"Terminating child process PID {child.pid}")
                        child.terminate()
                        child.wait(timeout=5)
                    except:
                        logging.error(f"Error terminating child process PID {child.pid}: {e}")
        except Exception as e:
            logging.error(f"Error finding/terminating child processes: {e}")

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
        client = MongoClient(__class__._MONGO_URL)
        db = client[__class__._DB_NAME]
        collection = db[__class__._COLLECTION_NAME]

        try:

            now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
            dateTime_str = now_ist.strftime("%Y-%m-%d %H:%M:%S")
            date_str = now_ist.date().strftime("%Y-%m-%d")
            time_str = now_ist.time().strftime("%H:%M:%S")

            for url in config.urls:
                # logging.info(f'Loaded config.cth_urls: {config.cth_urls}')
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
        client = MongoClient(__class__._MONGO_URL)
        db = client[__class__._DB_NAME]
        collection = db[__class__._COLLECTION_NAME]
        try:
            now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
            expiration_time = now_ist - timedelta(minutes=5)

            result = collection.delete_many({"datetime_obj" : {"$lte" : expiration_time}})
            logging.info(f"Deleted {result.deleted_count} old records from MongoDB.")
        except Exception as e:
            logging.error(f"Error while deleting expired data: {e}")

if __name__ =="__main__":
    win32serviceutil.HandleCommandLine(StreamSyncXService)
