import os
import queue
import time
import luigi
import argparse

from multiprocessing import Process
from task import RunPipelineTask


LIFOqueue = queue.LifoQueue(maxsize=3) 

# Function to listen for new files in a directory
def listen_for_new_data(trigger_dir):
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
                LIFOqueue.put(file_path)
                print(f"Added {file_path} to the queue.")
                
        time.sleep(2)  # Polling interval


def tasks_launcher():
    """
    Check the queue for new files and launch a new task for each file.
    """

    while True:
        if not LIFOqueue.empty():
            file_path = LIFOqueue.get()
            print(f"Processing {file_path}...")
            luigi.build([RunPipelineTask(data_path=file_path)], local_scheduler=False)
        else:
            time.sleep(2)  # Polling interval


def create_args_parser():
    parser = argparse.ArgumentParser(description="Start the pipeline in local mode")
    parser.add_argument("--trigger_dir", type=str, help="Directory to monitor for new data", default="./trigger_data", required=False)
    return parser.parse_args()


if __name__ == "__main__":
    # Ensure necessary directories exist
    os.makedirs("./logs", exist_ok=True)
    args = create_args_parser()

    # Start the listener process
    listener_process = Process(target=listen_for_new_data, args=(args.trigger_dir,))
    listener_process.start()

    # Start the task launcher
    tasks_launcher()
