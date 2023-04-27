"""Generate randomized health tracker data for following
Databricks tutorials on building a cloud data platform
https://www.youtube.com/watch?v=iLP571HBP6g&list=PLTPXxbhUt-YWyPmBDlFffnSJNrqIyla5F&index=2

Schema: {
  device_id: int
  heartrate: double
  name: str
  time: int
}
It intentionally includes bad data - in this case negative heart rates and
missing records.
"""

import argparse
from collections import namedtuple
import datetime as dt
import json
import pandas as pd
import matplotlib.pyplot as plt
import random
from typing import Optional

Device = namedtuple("Device", ["name", "num_records_missing"])

# Map device ID to name and number of missing records
DEVICES = {
    0: Device("Connor Chen", 0),
    1: Device("Harper Lee", 0),
    2: Device("Isaac Clarke", 0),
    3: Device("Niamh Gallagher", 0),
    4: Device("Xavier Martinez", 50),
}

HEARTRATE_MEAN = 60.0
HEARTRATE_STDDEV = 15.0
MINUTE = 60

# Defaults
DEFAULT_MONTH = "2020-01"
DEFAULT_NSECONDS = 5 * MINUTE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("outfile", help="The filename to store the generated data")
    parser.add_argument(
        "--month",
        default="2020-01",
        help="Timestamps within this month will be generated. Expected format YYYY-MM",
    )
    parser.add_argument(
        "--nseconds",
        default=DEFAULT_NSECONDS,
        help="The number of seconds of data to generate",
    )
    parser.add_argument(
        "--plot",
        action=argparse.BooleanOptionalAction,
        help="If true, generate plot of the heartrate vs timestamp",
    )
    cli_args = parser.parse_args()

    # Initialization
    random.seed()

    # Generation
    health_data = generate_health_tracking_data(
        filename=cli_args.outfile,
        start_ts=isomonth_to_timestamp(cli_args.month),
        nseconds=int(cli_args.nseconds),
    )
    if cli_args.plot:
        plot_heartrate_vs_timestamp_by_device(health_data)


def generate_health_tracking_data(filename: str, start_ts: int, nseconds: int) -> list:
    """Generate random health records and persist them to the given filename.
    A random month of the year is selected and a month of data is generated
    at the rate of 1 record/sec. Bad data is generated for each device by
    generating a negative heartrate at a random

    :param filename: The filename for the generated data
    :param start_ts: Unix timestamp defining the start of the records
    :param nseconds: The number of seconds of data to generate
    """

    health_data = []
    for device_id in DEVICES.keys():
        health_records(device_id, start_ts, nseconds, out=health_data)

    with open(filename, "w") as fd:
        json.dump(health_data, fd)

    return health_data


def isomonth_to_timestamp(month_as_str: str) -> int:
    """For a given month in the format 'YYYY-MM' generate a timestamp
    corresponding to midnight on the first day

    :param month: String formatted as YYYY-MM
    :return: A UNIX timestamp for midnight on the first day of the chosen month
    """
    # this is ntot robust to bad formatting!!
    year, month = map(int, month_as_str.split("-"))
    return int(dt.datetime(year, month, day=1).timestamp())


def health_records(
    device_id: int, start_ts: int, nseconds: int, out: Optional[list] = None
) -> dict:
    """Generate a set of records defining healh tracking data. In this case simply
    heartrate by device ID and name. Records are generated at a frequency of 1 per second.
    A bad record with a negative heartrate is written at a random index.
    There will also be records missing defined by the DEVICE_INFO mapping in the source code above.

    :param device_id: An integer identifying the device
    :param start_ts: Unix timestamp defining the start of the records
    :param nseconds: The number of seconds of data to generate
    :param out: If given then the records are appended to the given list, else a new list is created
    :return: The list containing the new data
    """
    data = [] if out is None else out
    # Entries are edges of the time regime so there will be 1 extra for the final edge.
    bad_data_index = random.randint(0, nseconds)
    nrecords = nseconds + 1 - DEVICES[device_id].num_records_missing
    for index in range(nrecords):
        heartrate = random.gauss(mu=HEARTRATE_MEAN, sigma=HEARTRATE_STDDEV)
        if index == bad_data_index:
            heartrate = -heartrate
        data.append(
            {
                "device_id": device_id,
                "heartrate": heartrate,
                "name": DEVICES[device_id].name,
                "timestamp": start_ts + index,
            }
        )

    return data


def plot_heartrate_vs_timestamp_by_device(health_records: list):
    """Show a plot of heartrate vs timestamp for each device"""
    df = pd.DataFrame.from_records(health_records)
    ax = None
    for device_id in DEVICES.keys():
        ax = df[df.device_id == device_id].plot(
            x="timestamp",
            y="heartrate",
            ax=ax,
            legend=True,
        )

    plt.show()


if __name__ == "__main__":
    main()
