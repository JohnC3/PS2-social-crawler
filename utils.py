import json
import logging
import os
import time
import urllib.request

with open('local_config.json', 'r') as json_conf:
    config_dict = json.load(json_conf)

LOGGER_NAME = 'ps2_crawler'
SERVICE_ID = config_dict['service_id']
DATABASE_PATH = os.path.join(*config_dict['database_path'])
RETRY_CAP = config_dict['retry_cap']
COOL_DOWN = config_dict['cool_down']
MAX_INACTIVE_DAYS = config_dict['max_inactive_days']
FRIEND_BATCH_SIZE = config_dict['friend_batch_size']


def setup_logger(level: str, log_file_name: str, server: str) -> None:
    """
    Setup a logger
    """

    if not log_file_name.endswith('.log'):
        log_file_name = log_file_name + '.log'

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    # Define the formatter
    log_format = f"{server}: %(levelname)s - %(asctime)s - %(message)s"
    formatter = logging.Formatter(log_format)
    # Define the console logging
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    stream.setLevel(logging.DEBUG)
    logger.addHandler(stream)

    # Define the log file we want to output to
    log_filer_handler = logging.FileHandler(log_file_name, mode='a')
    log_filer_handler.setFormatter(formatter)
    log_filer_handler.setLevel(logging.DEBUG)
    logger.addHandler(log_filer_handler)


def fetch_logger() -> logging.Logger:
    """
    Get the logger
    """

    return logging.getLogger(LOGGER_NAME)


def single_column(conn, query):
    return [i[0] for i in conn.execute(query).fetchall()]


def multi_column(conn, query):
    return [list(i) for i in conn.execute(query).fetchall()]


class GiveUpException(Exception):
    """
    Exceptions where we give up on a query immediately... TODO: Think of a better name
    """


def fetch_url(url: str) -> dict:

    # Fetch the data from url
    backoff = 4
    retry_count = 0
    while retry_count < RETRY_CAP:
        try:
            jsonObj = urllib.request.urlopen(url, timeout=30)
            decoded = json.loads(jsonObj.read().decode("utf8"))
            if 'errorCode' in decoded:
                if len(decoded.keys()) == 1:
                    fetch_logger().error(f"Sever returned an error {decoded} when running {url}")
                else:
                    fetch_logger().error(f"Sever partial error {decoded} when running {url}")
                raise GiveUpException(f"GiveUpException: because {decoded}")
            return decoded

        except GiveUpException as stop:
            # Don't handle such exceptions
            fetch_logger().error(f'Fatal exception raised {stop}')
            raise stop

        except (ConnectionResetError, Exception) as e:

            notice_str = f'While requesting {url} caught exception {e}; retry after {backoff}'
            fetch_logger().error(notice_str)
            time.sleep(backoff)
            backoff *= 2
            retry_count += 1
    raise Exception(f"Unable to call fetch_url on url {url} see logs")


def chunks(long_list, batch_size):
    """
    Breaks a list into a list of length batch_size lists.
    """
    list_of_batches = []

    while len(long_list) > batch_size:

        list_of_batches.append(long_list[:batch_size])
        long_list = long_list[batch_size:]

    if len(long_list) > 0:
        list_of_batches.append(long_list)
    return list_of_batches