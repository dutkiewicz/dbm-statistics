import logging
import os
from datetime import datetime, timedelta
from time import sleep

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, ConversionPixels, ConversionPixelsMetaNames
from core.util import DBMQuery, clean_date_value

CWD = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. run query with data from previous day
daterange = datetime.today() - timedelta(days=1)

dbm = DBMQuery(os.path.join(CWD, config('API_KEY_FILE')))
query_id = config('QUERY_CONVERSION_STATS')

logger.info("Running query {} with data from {}...".format(query_id, daterange))
dbm.run_query(query_id, 'CUSTOM_DATES', start_date=daterange, end_date=daterange, timezone="Europe/Warsaw")

# -----------------------------------------------------------------------------------
# 2. Fetch report from DBM API
# check if query is still running (exception RuntimeWarning). If not, proceed.
logger.info("Downloading report...")

while True:
    try:
        report = dbm.download_query(query_id, type='dict')
        logger.info("Downloaded!")
        break
    except RuntimeWarning:
        logger.info("Query is still running, waiting...")
        sleep(30)
        continue

# -----------------------------------------------------------------------------------
# 3. save report data to SQL
logger.info("Connecting to DB")
engine = create_engine(config('DB_URI'), echo=True)

# create all tables if they don't exist. If they do, SQL Alchemy skips creation
Base.metadata.create_all(engine)

# connect for inputing values
Session = sessionmaker(bind=engine)
session = Session()
conversion_ids_from_db = [x.conversion_id for x in session.query(ConversionPixelsMetaNames).all()]

for row in report:
    if row["Date"] == '':
        # every csv report contains summary and metadata that we don't need.
        # Fortunately, we can detect when summary row starts and disregard
        # everything after that in our loop
        logger.info("Reached csv file's metadata, breaking loop.")
        break

    if row['DV360 Activity'] != 'Total':  # 'Total' in report is a sum of all LI conversions and not needed
        if int(row['DV360 Activity ID']) not in conversion_ids_from_db:
            record_meta = ConversionPixelsMetaNames(conversion_id=row['DV360 Activity ID'],
                                                    conversion_name=row['DV360 Activity'])
            conversion_ids_from_db.append(int(row['DV360 Activity ID']))
        else:
            record_meta = session.query(ConversionPixelsMetaNames).filter_by(
                conversion_id=row['DV360 Activity ID']).first()
            record_meta.conversion_name = row['DV360 Activity']
        session.add(record_meta)

        record_stats = ConversionPixels(date=clean_date_value(row['Date']),
                                        line_item_id=row['Line Item ID'],
                                        conversion_id=row['DV360 Activity ID'],
                                        total_conversions=row['Total Conversions'],
                                        post_click_conversions=row['Post-Click Conversions'],
                                        post_click_revenue=row['CM Post-Click Revenue'],
                                        post_view_revenue=row['CM Post-View Revenue'])

        session.add(record_stats)

session.commit()
