import os
import re
import json
import requests
from bs4 import BeautifulSoup
import xarray as xr
import numpy as np
import pandas as pd
from cartopy import crs as ccrs, feature as cfeature
import matplotlib.pyplot as plt
import matplotlib
from pymongo import MongoClient, ReplaceOne
from google.cloud import storage
import thermofeel_fork.thermofeel as thermofeel
from utils import text_alert, load, open_dataset, erbs_ufunc, Database, Timer

# Helpful resource: [https://cloud.google.com/run/docs/quickstarts/jobs/build-create-python]
# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", "0")
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", "0")
# Retrieve User-defined env vars
MONGODB_URL = os.environ.get("MONGODB_URL")
CLOUD_MEDIA_STORAGE_BASE_URL = os.environ.get("CLOUD_MEDIA_STORAGE_BASE_URL")
CLOUD_MEDIA_STORAGE_ACCESS_KEY = os.environ.get("CLOUD_MEDIA_STORAGE_ACCESS_KEY")
LIMITS = json.loads(os.environ.get("LIMITS"))
CLOUD_DATA_STORAGE_BUCKET_NAME = os.environ.get("CLOUD_DATA_STORAGE_BUCKET_NAME")
DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL")


def main():
    db = Database(MongoClient(MONGODB_URL).w4h)
    timer = Timer()
    if db.status["isUpdating"]:
        print("ETL: Update already in progress. Exiting")
        return
    if not DATA_SOURCE_URL:
        print("ETL: checking for new data")
        previous_data_source = db.status["latestSuccessfulUpdateSource"]
        # ====== Web Scrape for link to latest available OPeNDAP data ======
        # ------ find latest date ------
        dates_directory = "https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"
        dates_source = requests.get(dates_directory, timeout=5000).text
        dates_soup = BeautifulSoup(dates_source, "lxml").body
        date_link_regex = r"^http:\/\/nomads\.ncep\.noaa\.gov(:80)?\/dods\/gfs_0p25_1hr\/gfs\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$"
        dates = dates_soup.find_all(href=re.compile(date_link_regex))
        del dates_source
        del dates_soup
        if not dates:
            return text_alert(f"ETL Error: zero dates found at {dates_directory}")
        latest_date = max(dates, key=lambda link: int(link.attrs["href"][-8:]))
        # ------ find latest time ------
        times_directory = latest_date.attrs["href"]
        times_source = requests.get(times_directory, timeout=5).text
        times_soup = BeautifulSoup(times_source, "lxml").body
        time_link_regex = r"^http:\/\/nomads\.ncep\.noaa\.gov(:80)?\/dods\/gfs_0p25_1hr\/gfs\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])\/gfs_0p25_1hr_(00|06|12|18)z\.info$"
        times = times_soup.find_all(href=re.compile(time_link_regex))
        if not times:
            return text_alert(f"ETL ERROR: found zero times at {times_directory}")
        latest_time = max(times, key=lambda link: int(link.attrs["href"][-8:-6]))
        # ------ get latest data link ------
        data_source = latest_time.attrs["href"][0:-5]
        timer.log("Scraped for updated data")

        if data_source == previous_data_source:
            print(f"ETL: Already using latest data from {previous_data_source}")
            return
        if db.fetch_status()["isUpdating"]:
            print("ETL: Update already in progress. Exiting")
            return
        print(
            f"ETL: updating data from {previous_data_source} to data from {data_source}"
        )
    else:
        data_source = DATA_SOURCE_URL
        print(f"ETL: updating using {data_source}")

    # ====== ETL Forecasts ======
    try:
        db.set_status("isUpdating", True)

        source = open_dataset(data_source)

        data_vars = [
            "tmp2m",
            "ugrd10m",
            "vgrd10m",
            "dpt2m",
            "dswrfsfc",
            "dlwrfsfc",
            "uswrfsfc",
            "ulwrfsfc",
        ]
        ds = xr.merge(
            [
                source[var]
                # exlude hour 0 due to missing solar data
                .isel(time=slice(1, 121)).sel(
                    lat=slice(LIMITS["south"], LIMITS["north"]),
                    lon=slice(LIMITS["west"], LIMITS["east"]),
                )
                for var in data_vars
            ]
        )
        # Sort
        # sort throws TypeError: cannot pickle '_thread.lock' object; try with Dask arrays instead
        # ds = ds.sortby(["lat", "lon", "time"]).transpose("lat", "lon", "time")
        ds = ds.transpose("lat", "lon", "time")

        # ====== Mean Radiant Temperature ======
        for var in ["uswrfsfc", "ulwrfsfc", "dlwrfsfc", "dswrfsfc"]:
            load(ds[var])

        # ------ calc avg_solar_cza ------
        # note: would normally divide by period, but period is already 1
        ds["avg_solar_cza"] = xr.concat(
            [
                xr.apply_ufunc(
                    thermofeel.calculate_cos_solar_zenith_angle_integrated,
                    ds.lat,
                    ds.lon,
                    time.dt.year,
                    time.dt.month,
                    time.dt.day,
                    time.dt.hour,
                    0,
                    1,
                )
                for time in ds.time
            ],
            "time",
        )

        # ------ calc direct_normal_irradiance ------
        # erbs(dswrfsfc, avg_solar_zenith, day_of_year)
        (
            ds["direct_normal_irradiance"],
            ds["diffuse_horizontal_irradiance"],
        ) = xr.apply_ufunc(
            erbs_ufunc,
            ds["dswrfsfc"],
            np.arccos(ds["avg_solar_cza"]),
            ds["time"].dt.dayofyear,
            output_core_dims=[(), ()],
        )

        # ------ calc mean_radiant_temp ------
        # calculate_mean_radiant_temperature(ssrd, ssr, dsrp, strd, fdir, strr, cossza)
        # fdir = dni, dsrp = solar direct through horizontal plane
        ds["mean_radiant_temp"] = xr.apply_ufunc(
            thermofeel.calculate_mean_radiant_temperature,
            ds.dswrfsfc,
            ds.dswrfsfc - ds.uswrfsfc,
            ds.dswrfsfc - ds.diffuse_horizontal_irradiance,
            ds.dlwrfsfc,
            ds.direct_normal_irradiance,
            ds.dlwrfsfc - ds.ulwrfsfc,
            ds.avg_solar_cza,
        )

        # ------ free memory -------
        # delete mean radiant tempeture components to save memory
        ds = ds.drop_vars(
            [
                "dswrfsfc",
                "dlwrfsfc",
                "uswrfsfc",
                "ulwrfsfc",
                "avg_solar_cza",
                "direct_normal_irradiance",
                "diffuse_horizontal_irradiance",
            ]
        )

        # ====== Wind Speed ======
        # load wind speed components
        for var in ["ugrd10m", "vgrd10m"]:
            load(ds[var])
        # calc wind speed from components
        ds["wind_speed"] = np.hypot(ds["ugrd10m"], ds["ugrd10m"])

        # delete wind speed components to save memory
        ds = ds.drop_vars(["vgrd10m", "ugrd10m"])

        # ====== UTCI, WBGT ======
        for var in ["tmp2m", "dpt2m"]:
            load(ds[var])
        # calculate_utci(t2_k, va_ms, mrt_k, ehPa, td_k)
        ds["utci"] = xr.apply_ufunc(
            thermofeel.calculate_utci,
            ds.tmp2m,
            ds.wind_speed,
            ds.mean_radiant_temp,
            None,
            ds.dpt2m,
        )
        # def calculate_wbgt(t_k, mrt, va, td, p=None)
        ds["wbgt"] = xr.apply_ufunc(
            thermofeel.calculate_wbgt,
            ds.tmp2m,
            ds.mean_radiant_temp,
            ds.wind_speed,
            ds.dpt2m,
        )
        # free memory
        ds = ds.drop_vars(["tmp2m", "dpt2m", "wind_speed", "mean_radiant_temp"])

        timer.log("Calculated forecasts")

        # download old data
        # Docs: [https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-python]
        cloud_storage = (
            storage.Client(project="weather-for-humans")
            .bucket(CLOUD_DATA_STORAGE_BUCKET_NAME)
            .blob("w4h_data.nc")
        )
        cloud_storage.download_to_filename("previous_w4h_data.nc", timeout=600)
        timer.log("downloaded previous data")

        # ====== Times ======
        # earliest global chart that can be updated with new data
        earliest_global_chart_to_update = pd.Timestamp(ds.time[0].item()).floor("d")
        # haa = hour-angle adjusted
        utc_now = pd.Timestamp.utcnow()
        # earliest haa utc-labeled date
        # i.e. what is the current utc-labelled date for longitudes of hour-angle -11
        earliest_current_haa_utc_date = (
            utc_now.tz_localize(None) - pd.Timedelta(11, "h")
        ).floor("d")
        # earliest date accessible to global charts (i.e. earliest utc-labeled 'yesterday')
        earliest_global_chart_date = earliest_current_haa_utc_date - pd.Timedelta(
            1, "d"
        )
        # earliest time necessary for charting (subtract 12 since data is shifted forwards up to 12 forwards)
        earliest_global_chart_data = earliest_global_chart_to_update - pd.Timedelta(
            12, "h"
        )
        # start of local day will always be within 25 hours of the current time (24 + 1 for daylight savings time)
        earliest_forecast_time = (
            (utc_now - pd.Timedelta(25, "h")).floor("d").tz_localize(None)
        )
        earliest_necessary_data = min(
            earliest_forecast_time, earliest_global_chart_data
        )

        # merge old with new
        ds = ds.combine_first(
            xr.open_dataset("previous_w4h_data.nc").sel(
                time=slice(earliest_necessary_data, None)
            )
        )
        # remove old data file to free memory
        os.remove("previous_w4h_data.nc")

        # ====== Data Upload =====
        # ------ Encoding ------
        # encode utci, max raw value 99.9 (encoded as 1999), min value -100.0 (encode as 0)
        ds["encoded_temp_times"] = ((ds.utci + 100) * 10).round().astype(np.int32)
        # encode wbgt
        ds["encoded_temp_times"] = ds.encoded_temp_times * 2000 + (
            (ds.wbgt + 100) * 10
        ).round().astype(np.int32)
        # encode time offset, max offset = 199 hrs (probs only need 120 + 50 but buffer just in case)
        ds["time_offset"] = ds.time - ds.time[0]
        # change timedelta64[ns] to integer number of hours.
        #
        # Note: must use accessors (dt.seconds, dt.days), otherwise strange
        # results on Ubuntu systems (dividing by nanoseconds results in
        # slightly off floats, conversion to integer appears to use
        # truncate rather than round resulting in offsets of
        # [0,0,2,3,3,5...]. See job w4h-etl-g8fzk)
        ds["time_offset"] = (
            (ds.time_offset.dt.seconds / 3600 + ds.time_offset.dt.days * 24)
            .round()
            .astype(int)
        )
        ds["encoded_temp_times"] = ds.encoded_temp_times * 200 + ds.time_offset

        # free memory
        ds = ds.drop_vars(["time_offset"])

        near_land = xr.open_dataarray("near_land_complete_globe.nc").sel(
            lat=ds.lat, lon=ds.lon
        )

        timer.reset()
        uploads = []
        forecast_start = ds.time[0].to_dict()["data"]
        for lat, lat_data, lat_near_land in zip(
            ds.lat.values, ds.encoded_temp_times.values, near_land.values
        ):
            for lon, data, is_near_land in zip(ds.lon.values, lat_data, lat_near_land):
                if is_near_land:
                    mongo_id = f"{lat:.2f},{lon:.2f}"
                    uploads.append(
                        ReplaceOne(
                            {"_id": mongo_id},
                            dict(
                                _id=f"{lat:.2f},{lon:.2f}",
                                forecastStart=forecast_start,
                                tempTimesEncoded=data.tolist(),
                            ),
                            upsert=True,
                        )
                    )
        timer.log("Created list of uploads")
        # END NEW

        # Need to upload in chunks, otherwise changing IP address of Google
        # Cloud Run results in broken connection. Can also fix by setting
        # up static IP address via NAT attached to VPC (costs $). So far,
        # always works for 12 chunks; including backups short-term
        for i in [12, 13, 15, 20, 30, 50, 100]:
            try:
                print(f"attempting upload in {i} parts")
                operations_chunked = np.array_split(uploads, i)
                for operations_chunk in operations_chunked:
                    db.upload_forecasts(operations_chunk.tolist())
                print(f"SUCCESS: uploaded in {i} parts")
                break
            except Exception as err:
                if i == 100:
                    raise err
                print(f"FAIL: upload in {i} parts failed")
        timer.log("Uploaded to Atlas")

        db.set_status("latestSuccessfulUpdateSource", data_source)

        # free memory
        ds = ds.drop_vars(["encoded_temp_times"])

        # save data to cloud storage
        timer.reset()
        ds.to_netcdf("new_w4h_data.nc")
        cloud_storage.upload_from_filename("new_w4h_data.nc", timeout=600)
        os.remove("new_w4h_data.nc")
        timer.log("Saved to cloud")

        # free memory
        ds = ds.drop_vars(["wbgt"])

        # ===== Charting =====
        # use non-gui backend for speed
        matplotlib.use("agg")
        # select utci data to chart and add cyclic when using worldwide data
        if 0 in ds.utci.lon:
            ds = xr.concat(
                [ds.utci, ds.utci.sel(lon=0).assign_coords(lon=360)], dim="lon"
            )
        else:
            ds = ds.utci

        # remove reference to charts older than earliest "yesterday" from status
        global_charts_removed = []
        for date in db.status["globalCharts"].keys():
            if pd.Timestamp(date) < earliest_global_chart_date:
                global_charts_removed.append(date)
                db.delete_from_status(f"globalCharts.{date}")
        if global_charts_removed:
            print(f"removed from status.globalCharts: {global_charts_removed}")

        # ------ shift data according to hour angle ------
        # haa = hour-angle-adjusted
        hour_angle = (ds.lon / 15).round().astype(int)
        utc_hour_angle = xr.where(hour_angle > 12, hour_angle - 24, hour_angle)
        for offset in np.unique(utc_hour_angle):
            # dataArray.where(condition, other) replaces values with other where condition is false
            ds = ds.where(utc_hour_angle != offset, other=ds.shift(time=offset))

        # prepare base chart
        colors = [
            "#004adb",
            "#306cde",
            "#468de0",
            "#5aadde",
            "#75cdd6",
            "#b3e8b6",
            "#ffde98",
            "#fcad6e",
            "#f27946",
            "#e43a20",
        ]
        divisions = [-40, -27, -13, 0, 9, 26, 32, 38, 46]

        projection = ccrs.Miller(central_longitude=7.625)
        fig = plt.figure(figsize=(20, 20))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
        ax.set_frame_on(False)
        ax.autoscale_view()
        ax.coastlines(linewidth=1, edgecolor="#000")
        ax.add_feature(cfeature.BORDERS, linewidth=0.7, edgecolor="#000")
        ax.add_feature(cfeature.STATES, linewidth=0.5, edgecolor="#000")
        plt.tight_layout(pad=0)

        source_info = re.search(
            r"\/gfs(\d{4})(\d{2})(\d{2}).*(\d{2}z)$", data_source
        ).groups()
        source_date = "-".join(source_info[:3])
        source_time = source_info[-1]
        timer.log("Prepared for charting")

        for date in np.unique(ds.time.dt.date):
            data = (ds.where(ds.time.dt.date == date, drop=True)).dropna("time")
            if len(data.time) < 24:
                continue
            for (vertex, data) in [
                ("highs", data.max("time")),
                ("lows", data.min("time")),
            ]:
                contour_set = data.plot.contourf(
                    ax=ax,
                    transform=ccrs.PlateCarree(),
                    add_labels=False,
                    add_colorbar=False,
                    levels=divisions,
                    colors=colors,
                )
                # create image file
                file_name = f"{date}Z_utci_{vertex}_from_gfs_data_up_to_{source_date}_{source_time}.png"
                fig.savefig(f"{file_name}", dpi=100, pad_inches=0, bbox_inches="tight")

                # clear plotted contours for next chart (results in faster charting - no need to create new figure and features)
                for contour in contour_set.collections:
                    contour.remove()

                # upload file to cloud storage
                upload_url = f"{CLOUD_MEDIA_STORAGE_BASE_URL}/{date}Z/{file_name}"
                with open(f"{file_name}", "rb") as upload:
                    res = requests.put(
                        upload_url,
                        headers={"AccessKey": CLOUD_MEDIA_STORAGE_ACCESS_KEY},
                        data=upload,
                        timeout=300,
                    )
                if res.status_code != 201:
                    print(f"error uploading {file_name}")
                else:
                    # update status
                    db.set_status(
                        f"globalCharts.{date}", f"{source_date}_{source_time}"
                    )
                # delete image file
                os.remove(f"{file_name}")
                timer.log(f"Charted {date} {vertex}")
        # upon ETL completion
        print(f"ETL: completed using: {data_source}")

    finally:
        db.set_status("isUpdating", False)


# Start script
if __name__ == "__main__":
    main()
    # try:
    #     main()
    # except Exception as err:
    #     message = (
    #         f"Task #{TASK_INDEX}, " + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"
    #     )
    #     print(json.dumps({"message": message, "severity": "ERROR"}))
    #     sys.exit(1)  # Retry Job Task by exiting the process
