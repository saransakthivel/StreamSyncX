import os
import win32serviceutil
import win32service
import win32event
import subprocess
from subprocess import Popen, PIPE
import logging
from logging.handlers import RotatingFileHandler

# Logging setup --> start

logging.basicConfig(level=logging.INFO)

log_directory = "C:\\StreamSyncX\\logs"
log_file = os.path.join(log_directory, "dramatiq_worker_service.log")

os.makedirs(log_directory, exist_ok=True)

log_handler = RotatingFileHandler(
    log_file, maxBytes=1 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
log_handler.setLevel(logging.INFO)

log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

logging.getLogger().addHandler(log_handler)

# logging setup --> end


logging.info("PSG Dramatic Task Service: Initialization started.")

class TaskService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PsgDramaticTaskService"
    _svc_display_name_ = "PSG Dramatic Task Service"  
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.process = None
    
    def SvcStop(self):
        logging.info("Service is shutting down...")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        logging.info("Service stopped successfully.")
        
        if self.process:
            logging.info("Stopping subprocess...")
            self.process.terminate() 
            try:
                self.process.wait(timeout=5)
                logging.info("Subprocess Stopped Successfully.")
            except subprocess.TimeoutExpired:
                logging.warning("Process did not terminate in time, forcing kill")
                self.process.kill()
                
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)
        logging.info("Service stopped successfully.")
    
    def SvcDoRun(self):
        logging.info("Service is now running.")
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        
        os.chdir("C:\\StreamSyncX")
        os.environ["MY_ENV_VAR"] = "value"
        
        try:
            logging.info("Starting Dramatiq worker process...")
            self.process = Popen(
                ['python', '-m', 'dramatiq', 'tasks'], 
                stdout=PIPE, 
                stderr=PIPE
            )
        except Exception as e:
            logging.error(f"Error starting Dramatiq worker: {str(e)}")
        
        while True:
            result = win32event.WaitForSingleObject(self.stop_event, 5000)  # 5 seconds check interval
            if result == win32event.WAIT_OBJECT_0:
                break

            output = self.process.stdout.read()
            if output:
                logging.info(f"Worker Output: {output.decode()}")

            error = self.process.stderr.read()
            if error:
                logging.error(f"Worker Error: {error.decode()}")

        self.process.terminate()
        logging.info("Dramatiq worker process terminated.")
        
if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(TaskService)
