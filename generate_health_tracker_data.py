"""Generate randomized health tracker data for following
Databricks tutorials on building a cloud data platform
https://www.youtube.com/watch?v=iLP571HBP6g&list=PLTPXxbhUt-YWyPmBDlFffnSJNrqIyla5F&index=2

Schema: {
  device_id: int
  heartrate: double
  name: str
  time: int
}
"""

import argparse
import calendar
import datetime as dt
import json
import random
from typing import Optional


DEVICE_NAME_MAPPING = {
0:"Connor Chen",
1:"Harper Lee",
2:"Isaac Clarke",
3:"Niamh Gallagher",
4:"Xavier Martinez",
5:"Emily Nguyen",
6:"Jackson Reyes",
7:"Sarah Kim",
8:"Aiden Patel",
9:"Mia Rodriguez"
}

HEARTRATE_MEAN = 60.
HEARTRATE_STDDEV = 15.

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("outfile", help="The filename to store the generated data")
    parser.add_argument("--nrecords_per_owner", default=1000, help="Generate this number of timestamped entries per device owner")
    parser.add_argument("--year", default=2020, help="Timestamps within this year will be generated")
    cli_args = parser.parse_args()

    random.seed()
    generate_health_tracking_data(filename=cli_args.outfile, nrecords_per_owner=cli_args.nrecords_per_owner, year=cli_args.year)


def generate_health_tracking_data(filename: str, nrecords_per_owner: int, year: int) -> None:
    """Generate random healh records and persist them to the given filename"""
    health_data = []
    for device_id in DEVICE_NAME_MAPPING.keys():
        health_records(year, device_id, nrecords_per_owner, out=health_data)

    with open(filename, 'w') as fd:
        json.dump(health_data, fd)

def health_records(year: int, device_id: int, nrecords: int, out: Optional[list]=None) -> dict:
    """Generate a number of records health data in the given year.
    The first date/time is chosen randomly and incremented 1 second
    for each entry.

    :param year: An int specifying the year for timestamps
    :param device_id: An integer identifying the device
    :param nrecords: The number of records to generate
    :param out: If given then the records are appended to the given list, else a new list is created
    :return: The list containing the new data
    """
    if calendar.isleap(year):
        day = random.randint(1, 365)
    else:
        day = random.randint(1, 366)

    hour_start_dt = dt.datetime(year, month=1, day=1, hour=random.randint(0, 23)) + dt.timedelta(days=day)
    hour_start_ts = int(hour_start_dt.timestamp())
    data = [] if out is None else out
    for index in range(nrecords):
        heartrate = random.gauss(mu=HEARTRATE_MEAN)
        data.append({"device_id": device_id,
                     "heartrate": heartrate,
                     "name": DEVICE_NAME_MAPPING[device_id],
                     "timestamp": hour_start_ts + index}
        )

    return data

if __name__ == "__main__":
    main()
