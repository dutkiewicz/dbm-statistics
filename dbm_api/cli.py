import os
import argparse
import csv

import requests
from decouple import config
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from util import DBMQuery, clean_date_value, clean_money_value
from models import BasicStats, Base

# parse arguments from CLI
parser = argparse.ArgumentParser(description="Set flags for your download")
parser.add_argument('api_key', help="Path to API key in JSON")
parser.add_argument('-l', '--list', action='store_true', help='List available queries')
parser.add_argument('-d', '--download-report', type=int, nargs=1, help='Download report file based on query ID')
parser.add_argument('-s', '--save-report', type=int, nargs=1, help='Save CSV file to database. ' \
                                                                               'Requires preconfigured SQL database!')
parser.add_argument('-r', '--run-query', nargs=2, help='Run query ID (1) with data from date range (2).')
parser.add_argument('-c', '--create-query', type=str, nargs=1, help='Create new Query from JSON file')
parser.add_argument('--remove-query', nargs='*', help='Delete queries from DBM')
args = parser.parse_args()

DIR = os.path.dirname(os.path.abspath(__file__))

if os.path.exists(args.api_key):
    dbm = DBMQuery(args.api_key)
else:
    raise OSError("API key {} does not exist".format(args.api_key))

if args.list:
    dbm.list_queries()

if args.download_report:
    print("Downloading Query {}".format(args.download_report))

    query = requests.get(dbm.download_query(args.download_report[0]))
    filename = str(args.download_report[0]) + '.csv'

    print("Saving file...")
    with open(os.path.join(DIR, 'reports', filename), 'wb') as file:
        file.write(query.content)
    print("File saved!")

if args.save_report:
    query = requests.get(dbm.download_query(args.save_report[0], dbm))
    filename = str(args.save_report[0]) + '.csv'

    # save report data to SQL
    engine = create_engine(config('DB_URI'))

    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # csv file
    csv_file = os.path.join('reports', filename)

    with open(csv_file) as file:
        report = csv.DictReader(file)

        for counter, row in enumerate(report, 1):
            if len(row["Date"]) == 0: # DBM csv reports have meta info at the bottom of file. We have to skip them
                break
            else:
                record = BasicStats(date = clean_date_value(row["Date"]),
                                    advertiser = row["Advertiser"],
                                    advertiser_id = row["Advertiser ID"],
                                    insertion_order = row["Insertion Order"],
                                    insertion_order_id = row["Insertion Order ID"],
                                    line_item = row["Line Item"],
                                    line_item_id = row["Line Item ID"],
                                    currency = row["Advertiser Currency"],
                                    impressions = row['Impressions'],
                                    viewable_impressions = row["Active View: Viewable Impressions"],
                                    clicks = row['Clicks'],
                                    total_conversions = row["Total Conversions"],
                                    total_cost = clean_money_value(row["Total Media Cost (Advertiser Currency)"]))
                session.add(record)
                session.commit()

            print("Saved row {}".format(counter))

if args.run_query:
    dbm.run_query(args.run_query[0], args.run_query[1])

if args.create_query:
    dbm.create_query(args.create_query[0])

if args.remove_query:
    for id in args.remove_query:
        dbm.delete_query(id)
        print("Removed Query ID {}".format(id))