import os
import time
import luigi
import logging
import argparse
import threading

from multiprocessing import Process
from multiprocessing.managers import BaseManager
from task import RunPipelineTask

class SimpleLIFO(object):
    """
    Simple LIFO queue.
    """
    def __init__(self):
        self.var = []
        self.lock = threading.Lock()

    def put(self, value):
        with self.lock:
            self.var.append(value)

    def get(self):
        with self.lock:
            if not self.empty():
                value = self.var.pop()
                return value
            else:
                return None

    def empty(self):
        with self.lock:
            return len(self.var) == 0

    def len(self):
        with self.lock:
            return len(self.var)


# Function to listen for new files in a directory
def listen_for_new_data(trigger_dir: str, Queue: SimpleLIFO):
    """Listen to a directory for new files and add them to the queue.

    Args:
        trigger_dir (str): directory to monitor for new files
        Queue (SimpleLIFO): queuing object to store new files
    """
    print(f"Listening for new files in {trigger_dir}...")
    processed_files = set()
    while True:
        # Check for new files
        for filename in os.listdir(trigger_dir):
            file_path = os.path.join(trigger_dir, filename)
            if file_path not in processed_files and os.path.isfile(file_path):
                print(f"New file detected: {file_path}")

                # Mark file as processed
                processed_files.add(file_path)

                # Add it to the queue
                Queue.put(file_path)
                print(f"Added {file_path} to the queue.")
        time.sleep(2)  # Polling interval


def tasks_launcher(Queue: SimpleLIFO):
    """
    Check the queue for new files and launch a new task for each file.
    """
    print("Starting task launcher...")
    while True:
        if not Queue.empty():
            file_path = Queue.get()
            print(f"Processing {file_path}...")
            luigi.build([RunPipelineTask(data_path=file_path)], local_scheduler=False)
        else:
            time.sleep(2)


def create_args_parser():
    parser = argparse.ArgumentParser(description="Start the pipeline in local mode")
    parser.add_argument("--trigger_dir", type=str, help="Directory to monitor for new data", default="./trigger_data", required=False)
    return parser.parse_args()

if __name__ == "__main__":
    # Ensure necessary directories exist
    os.makedirs("./logs", exist_ok=True)
    args = create_args_parser()

    # Register the SimpleLIFO class with the manager
    BaseManager.register('SimpleLIFO', SimpleLIFO)
    manager = BaseManager()
    manager.start()
    inst = manager.SimpleLIFO()

    # Start the listener process
    listener_process = Process(target=listen_for_new_data, args=(args.trigger_dir,inst))
    listener_process.start()

    # Start the task launcher
    tasks_launcher_process = Process(target=tasks_launcher, args=(inst,))
    tasks_launcher_process.start()

    # Wait for the processes to finish
    tasks_launcher_process.join()
    listener_process.join()
    print("All done!")
