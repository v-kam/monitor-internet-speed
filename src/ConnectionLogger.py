import speedtest
import time
import csv
import os
import threading
from typing import Tuple, List
from datetime import datetime
import pandas as pd
import logging

# Configure the logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the timestamp format
)


class ConnectionLogger(threading.Thread):
    def __init__(self, outpath="connection_log.csv", sleep_sec=60) -> None:
        super().__init__()  # Initialize the Thread base class
        self.outpath = outpath
        self.sleep_sec = sleep_sec
        self.results: List[dict] = []
        self._stop_event = threading.Event()  # Event to signal the thread to stop
        self._results_lock = threading.Lock()  # Lock to synchronize access to self.results
        logging.info("Logger started.")

    def conduct_speed_test(self) -> Tuple[int, float, float, str]:
        """
        Conduct a speed test and return the timestamp, download speed, upload speed, and WAN IP.
        https://github.com/sivel/speedtest-cli/wiki
        """
        logging.info("Conducting speed test.")
        sys_time = datetime.now().replace(microsecond=0)
        s = speedtest.Speedtest(secure=True)
        s.get_servers()
        s.get_best_server()
        s.download()
        s.upload()
        s.results.share()
        results:dict = s.results.dict()
        results["system_time"] = sys_time
        return results

    def log_speedtest_to_csv(self, result:dict) -> None:
        """Log the speed test results to a CSV file in the desired schema."""     
        # Define the row data according to the schema
        row = self.unnest_results(result)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.outpath), exist_ok=True)

        with open(self.outpath, mode="a", newline="") as speedcsv:
            csv_writer = csv.DictWriter(speedcsv, fieldnames=row.keys())
            if speedcsv.tell() == 0:
                csv_writer.writeheader()
            csv_writer.writerow(row)

    def clip_results(self, max_size: int = 9240) -> None:
        """Clip self.results to the most recent max_size elements."""
        with self._results_lock:
            if len(self.results) > max_size:
                self.results = self.results[-max_size:]

    def run(self) -> None:
        """Override the run method from threading.Thread to execute the logger's logic."""
        while not self._stop_event.is_set():
            data = self.conduct_speed_test()
            with self._results_lock:
                self.results.append(data)
            self.clip_results()  # Ensure results list does not grow too large
            self.log_speedtest_to_csv(data)
            time.sleep(self.sleep_sec)

    def stop(self) -> None:
        """Stop the connection logger thread."""
        self._stop_event.set()
        logging.info("Logger stopped.")

    def get_results(self) -> pd.DataFrame:
        """Thread-safe method to get a copy of the results as a formatted DataFrame."""
        with self._results_lock:
            rows = [self.unnest_results(result) for result in self.results]
            df = pd.DataFrame(rows)
            return df

    @staticmethod
    def unnest_results(result:dict) -> dict:
        """Unnest and convert speed test result into a flat dictionary following the defined schema."""
        download_mbps = round((round(result.get('download')) / 1048576), 2)
        upload_mbps = round((round(result.get('upload')) / 1048576), 2)

        row = {
            'system_time': result.get('system_time'),
            'download': download_mbps,
            'upload': upload_mbps,
            'ping': result.get('ping'),
            'server_url': result['server'].get('url'),
            'server_lat': float(result['server'].get('lat')),
            'server_lon': float(result['server'].get('lon')),
            'server_name': result['server'].get('name'),
            'server_cc': result['server'].get('cc'),
            'server_id': result['server'].get('id'),
            'server_d': result['server'].get('d'),
            'server_latency': result['server'].get('latency'),
            'timestamp': result.get('timestamp'),
            'bytes_sent': result.get('bytes_sent'),
            'bytes_received': result.get('bytes_received'),
            'share': result.get('share'),
            'client_ip': result['client'].get('ip'),
            'client_lat': float(result['client'].get('lat')),
            'client_lon': float(result['client'].get('lon')),
            'client_isp': result['client'].get('isp'),
            'client_isprating': result['client'].get('isprating'),
            'client_rating': result['client'].get('rating'),
            'client_ispdlavg': result['client'].get('ispdlavg'),
            'client_ispulavg': result['client'].get('ispulavg'),
            'client_loggedin': bool(int(result['client'].get('loggedin'))),
            'client_country': result['client'].get('country')
        }

        return row



# Example usage:
if __name__ == "__main__":
    """
    This runs the Connection Logger parrallel to another thread that reads
    the captured information.
    """
    logger = ConnectionLogger(
        outpath="logs/connection_log_test.csv",
        sleep_sec=2
    )
    logger.start()

    # Example of accessing the results from another thread
    def print_results():
        while True:
            time.sleep(4)  # Print results every 10 seconds
            results:pd.DataFrame = logger.get_results()
            print(f"Getting results (empty if none there yet) {logger.is_alive()=}:")
            print(results)

    # Start the thread that prints the results
    printer_thread = threading.Thread(target=print_results)
    printer_thread.start()

    # Run the logger for a certain period, then stop it (for demonstration purposes)
    try:
        time.sleep(300)  # Let it run for ten minutes
    finally:
        logger.stop()
        logger.join()  # Ensure the logger thread has finished execution
        printer_thread.join()  # Ensure the printer thread has finished execution
