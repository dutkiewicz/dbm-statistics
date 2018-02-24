from time import sleep
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.models import Base, MetaNames, BasicStats
from core.util import DBMQuery, clean_date_value, clean_currency_value


# 1. run query with PREVIOUS_DAY data
dbm = DBMQuery(config('API_KEY_FILE'))
query_id = config('QUERY_BASIC_STATS')

dbm.run_query(query_id, 'PREVIOUS_DAY')

# -----------------------------------------------------------------------------------
# 2. Fetch report from DBM API
# check if query is still running (exception RuntimeWarning). If not, proceed.
is_running = True
while is_running:
    try:
        report = dbm.download_query(query_id, type='dict')
        is_running = False
    except RuntimeWarning:
        sleep(60)
        continue

# -----------------------------------------------------------------------------------
# 3. save report data to SQL

engine = create_engine(config('DB_URI'), echo=True)

# create all tables if they don't exist. If they do, SQL Alchemy skips creation
Base.metadata.create_all(engine)

# connect for inputing values
Session = sessionmaker(bind=engine)
session = Session()

# add MetaNames first
for row in report:
    if row["Date"] != '':
        # every csv report contains summary and metadata that we don't need.
        # Fortunately, we can detect when summary row starts and disregard
        # everything after that in our loop

        # query database to check if our line items already exists
        # if so, then skip
        db_line_items = [x.line_item_id for x in session.query(MetaNames.line_item_id).all()]

        if int(row['Line Item ID']) not in db_line_items:
            rec_meta = MetaNames(advertiser_name = row['Advertiser'],
                                 advertiser_id = row['Advertiser ID'],
                                 order_name = row['Insertion Order'],
                                 order_id = row['Insertion Order ID'],
                                 line_item_name = row['Line Item'],
                                 line_item_id = row['Line Item ID'])
            session.add(rec_meta)
        else:
            # pass adding Line Item if it already exists in database
            pass

        # add BasicStats
        rec_stats = BasicStats(date=clean_date_value(row['Date']),
                               line_item_id=row['Line Item ID'],
                               currency=row['Advertiser Currency'],
                               impressions=row['Impressions'],
                               viewable_impressions=row['Active View: Viewable Impressions'],
                               clicks=row['Clicks'],
                               total_conversions=row['Total Conversions'],
                               post_click_conversions=row['Post-Click Conversions'],
                               total_cost=clean_currency_value(row['Total Media Cost (Advertiser Currency)']),
                               media_cost=clean_currency_value(row['Media Cost (Advertiser Currency)'])
                               )

        session.add(rec_stats)
    else:
        # break iteration if we reach csv summary row
        break

session.commit()
