import os
import time
from datetime import timedelta
import xarray as xr
import requests
from pvlib.irradiance import erbs
from retry import retry
from pymongo.errors import AutoReconnect

TEXTBELT_API_KEY = os.environ.get("TEXTBELT_API_KEY")
ADMIN_PHONE_NUMBER = os.environ.get("ADMIN_PHONE_NUMBER")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY")


def text_alert(message):
    res = requests.post(
        "https://textbelt.com/text",
        {
            "phone": ADMIN_PHONE_NUMBER,
            "message": message,
            "key": TEXTBELT_API_KEY,
            "sender": "W4H",
        },
        timeout=5000,
    ).json()
    if res["success"] and res["quotaRemaining"] == 1:
        text_alert(
            "W4H Alert: need to purchase more text credits for textbelt.com SMS API"
        )
    raise Exception(message)


@retry(RuntimeError)
def retry_load(dataArray):
    return dataArray.load()


def load(dataArray):
    try:
        retry_load(dataArray)
    except Exception as ex:
        raise Exception(f"ETL: Unable to load {dataArray.name} data") from ex


@retry(Exception)
def retry_open_dataset(url):
    return xr.open_dataset(url)


def open_dataset(url):
    try:
        return retry_open_dataset(url)
    except Exception as ex:
        raise Exception("ETL: Unable to open dataset") from ex


# wrapper necessary for apply_ufunc, which expects return value = array or tuple of arrays
def erbs_ufunc(ghi, zenith, doy):
    erbs_results = erbs(ghi, zenith, doy)
    return (erbs_results["dni"], erbs_results["dhi"])


# ------ Status ------
class Database:
    def __init__(self, db):
        self.db = db
        self.status = None
        self.fetch_status()

    # All database operations are decorated to retry upon AutoReconnect
    # error, which is thrown when there is a connection failure and the
    # next database opperation will first attempt to create a new
    # connection. See
    # [https://pymongo.readthedocs.io/en/stable/api/pymongo/errors.html#pymongo.errors.AutoReconnect]
    # and [https://stackoverflow.com/questions/28809168/why-does-pymongo-throw-autoreconnect]
    @retry(AutoReconnect, tries=2, delay=0)
    def upload_forecasts(self, operation):
        self.db.forecasts.bulk_write(operation, ordered=False)

    @retry(AutoReconnect, tries=2, delay=0)
    def set_status(self, field, value):
        self.db.status.update_one({"_id": "status"}, {"$set": {field: value}})

    @retry(AutoReconnect, tries=2, delay=0)
    def fetch_status(self):
        self.status = self.db.status.find_one({"_id": "status"})
        return self.status

    @retry(AutoReconnect, tries=2, delay=0)
    def delete_from_status(self, field):
        self.db.status.update_one({"_id": "status"}, {"$unset": {field: ""}})


# For testing
class Timer:
    def __init__(self):
        self.reset()

    def reset(self):
        self.wall_start = time.time()
        self.process_start = time.process_time()

    def diff(self):
        return (time.time() - self.wall_start, time.process_time() - self.process_start)

    def log(self, description):
        elapsed_wall, elapsed_process = self.diff()
        print(
            f"TIMER: {description}: elapsed wall time: {timedelta(seconds=elapsed_wall)}; elapsed process time: {timedelta(seconds=elapsed_process)}"
        )
        self.reset()
