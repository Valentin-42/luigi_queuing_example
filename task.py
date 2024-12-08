
import luigi
import datetime
import logging
import time


# Luigi task for pipeline execution
class RunPipelineTask(luigi.Task):
    data_path = luigi.Parameter()

    def run(self):
        timestamp = datetime.datetime.now().strftime("%Hh_%Mm_%Ss-%d_%m_%Y")
        log_file = f"./logs/{timestamp}.log"
        
        # Configure the logger
        logging.basicConfig(
            filename=log_file,               # Specify the log file name
            level=logging.INFO,               # Set the logging level to INFO
            format='%(asctime)s - %(levelname)s - %(message)s'  # Define the log message format
        )
        
        logging.info('Starting a new task...')
        
        logging.info(f"Processing data from {self.data_path}...")

        # Simulate long-running pipeline task
        time.sleep(5)  # Simulate processing delay

        logging.info(f"Pipeline processing complete for {self.data_path}.\n")

    def output(self):
        return luigi.LocalTarget(f"{self.data_path}/output.txt")
