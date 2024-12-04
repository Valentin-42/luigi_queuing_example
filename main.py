import os
import time
import argparse
import luigi
import subprocess
import datetime

# Luigi task for pipeline execution
class RunPipelineTask(luigi.Task):
    data_path = luigi.Parameter()

    def run(self):
        timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        log_file = f"./logs/log_{timestamp}.txt"
        
        with open(log_file, 'w+') as f:
            f.write('Start new task...\n')
            f.flush()

            # Simulate running the pipeline locally
            f.write(f"Processing data from {self.data_path}...\n")
            f.flush()

            # Simulate long-running pipeline task
            time.sleep(5)  # Simulate processing delay

            f.write(f"Pipeline processing complete for {self.data_path}.\n")
            f.flush()

    def output(self):
        return luigi.LocalTarget(f"{self.data_path}/output.txt")


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

                # Run the Luigi pipeline for the new data
                luigi.build([RunPipelineTask(data_path=file_path)], local_scheduler=False)

                print(f"Pipeline finished for {file_path}, ready for next task.")

        time.sleep(2)  # Polling interval


def create_args_parser():
    parser = argparse.ArgumentParser(description="Start the pipeline in local mode")
    parser.add_argument("--trigger_dir", type=str, help="Directory to monitor for new data", default="./trigger_data", required=False)
    return parser.parse_args()


if __name__ == "__main__":
    # Ensure necessary directories exist
    os.makedirs("./logs", exist_ok=True)
    args = create_args_parser()

    listen_for_new_data(trigger_dir=args.trigger_dir)
