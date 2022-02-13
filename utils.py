import json
import logging
import os
import time
import urllib.request

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

with open('local_config.json', 'r') as json_conf:
    config_dict = json.load(json_conf)

SERVICE_ID = config_dict['service_id']
DATABASE_PATH = os.path.join(*config_dict['database_path'])
RETRY_CAP = config_dict['retry_cap']
COOL_DOWN = 5.0
MAX_INACTIVE_DAYS = 21
FRIEND_BATCH_SIZE = 40


def single_column(conn, query):
    return [i[0] for i in conn.execute(query).fetchall()]


def multi_column(conn, query):
    return [list(i) for i in conn.execute(query).fetchall()]


class GiveUpException(Exception):
    """
    Exceptions where we give up on a query immediately... TODO: Think of a better name
    """


def fetch_url(url: str):

    # Fetch the data from url
    backoff = 4
    retry_count = 0
    while retry_count < RETRY_CAP:
        try:
            jsonObj = urllib.request.urlopen(url, timeout=30)
            decoded = json.loads(jsonObj.read().decode("utf8"))
            if 'errorCode' in decoded:
                if len(decoded.keys()) == 1:
                    logger.error(f"Sever returned an error {decoded} when running {url}")
                else:
                    logger.error(f"Sever partial error {decoded} when running {url}")
                raise GiveUpException(f"GiveUpException: because {decoded}")
            return decoded

        except GiveUpException as stop:
            # Don't handle such exceptions
            raise stop

        except Exception as e:
            if 'WinError 10054' in str(e):
                # The connection has been terminated, don't know why, but stop harassing the server about it.
                raise GiveUpException(str(e))

            notice_str = 'While requesting {} caught exception {}; retry after {}'.format(url, e, backoff)
            if backoff < 10:
                logger.debug(notice_str)
            else:
                logger.info(notice_str)
            time.sleep(backoff)
            backoff *= 2
            retry_count += 1
    raise Exception(f"Unable to call fetch_url on url {url} see logs")



