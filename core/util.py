from datetime import datetime
from httplib2 import Http
import json
import re
import csv
import io
import pytz
import requests
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build


class DBMQuery():
    """
    Additional layer for simplifying mundane interactions with DBM API.
    """

    def __init__(self, auth_json):
        """
        authorize http requests for service account
        """
        self.auth_json = auth_json
        self.scope = ['https://www.googleapis.com/auth/doubleclickbidmanager']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.auth_json, self.scope)
        self.http_auth = self.credentials.authorize(Http())

        # client is Google's API client class
        self.client = build('doubleclickbidmanager', 'v1', http=self.http_auth)


    def run_query(self, query_id, daterange, start_date=None, end_date=None, timezone='America/New_York'):
        """
        Run pre-compiled query in DBM. DOES NOT SUPPORT CUSTOM DATES YET!
        :param query_id: QueryID in DBM
        :param daterange: predefined date ranges in DBM (see documentation: https://developers.google.com/bid-manager/v1/queries
        :param start_date: if daterange == "CUSTOM_DATES" then provide report start date in YYYY-MM-DD format
        :param end_date: if daterange == "CUSTOM_DATES" then provide report end date in YYYY-MM-DD format
        :param timezone: Canonical timezone code for report data time. Defaults to America/New_York.
        :return: json response from DBM
        """
        DATE_RANGE = ("ALL_TIME",
                      "CURRENT_DAY",
                      "CUSTOM_DATES",
                      "LAST_14_DAYS",
                      "LAST_30_DAYS",
                      "LAST_365_DAYS",
                      "LAST_7_DAYS",
                      "LAST_90_DAYS",
                      "MONTH_TO_DATE",
                      "PREVIOUS_DAY",
                      "PREVIOUS_HALF_MONTH",
                      "PREVIOUS_MONTH",
                      "PREVIOUS_QUARTER",
                      "PREVIOUS_WEEK",
                      "PREVIOUS_YEAR",
                      "QUARTER_TO_DATE",
                      "TYPE_NOT_SUPPORTED",
                      "WEEK_TO_DATE",
                      "YEAR_TO_DATE"
                      )

        def date_to_miliseconds(date):
            try:
                utc = pytz.timezone(timezone) #assert that we're using correct timezone for DBM
                miliseconds = round(datetime.timestamp(utc.localize(date)) * 1000)
                return miliseconds
            except ValueError:
                raise ValueError("{} is not a valid format, please provide" \
                                 " date object.".format(str))


        if daterange in DATE_RANGE:
            body = {
                "dataRange": daterange,
                "timezoneCode": timezone
            }

            if daterange == 'CUSTOM_DATES':
                body.update({
                    "reportDataStartTimeMs": date_to_miliseconds(start_date),
                    "reportDataEndTimeMs": date_to_miliseconds(end_date)
                })


            return self.client.queries().runquery(queryId=query_id, body=body).execute()

        else:
            raise ValueError("{} is not within approved dateranges. "
                             "Check https://developers.google.com/bid-manager/v1/queries for more information".format(daterange))


    def list_queries(self):
        """
        List available created queries in DBM
        :param client: QueryID in DBM
        :return: print queries to stdout
        """
        query = self.client.queries().listqueries().execute()

        try:
            print("Query ID", "Name", "Data Range", "Last run date", "Is running?", sep=" | ")
            for q in query['queries']:
                print(q['queryId'],
                      q['metadata']['title'],
                      q['metadata']['dataRange'],
                      datetime.fromtimestamp(int(q['metadata']['latestReportRunTimeMs']) / 1000.0).strftime(
                          '%Y-%m-%d %H:%M:%S'),
                      q['metadata']['running'],
                      sep=" | ")
        except KeyError:
            print("No queries are created for this account. Use -c to create new one.")
            exit(0)

    def create_query(self, file):
        """
        Create new query from json.
        :param body: json with query parameters
        :param client: DBM authorized client
        :return: empty Http response
        """
        try:
            with open(file) as file:
                body = json.load(file)
        except AttributeError:
            raise AttributeError("{} is not a file, please specify path to json file".format(body))
        except json.decoder.JSONDecodeError:
            raise TypeError("{} is not a json file".format(body))

        return self.client.queries().createquery(body=body).execute()


    def get_query_url_to_file(self, query_id):
        """
        Returns URL to existing report in DBM
        :param query_id: QueryID in DBM
        :return: URL to file
        """
        query = self.client.queries().getquery(queryId=query_id).execute()

        if not query:
            raise ValueError("Query ID {} does not exist in DBM. Have you run query before downloading it?".format(query_id))
        elif query['metadata']['running']:
            raise RuntimeWarning("Query ID {} is still running!".format(query_id))
        else:
            return query['metadata']['googleCloudStoragePathForLatestReport']

    def download_query(self, query_id, type='content'):
        """
        Returns Http request's raw data
        :param query_id: QueryID in DBM
        :param type: raw - request.raw; csv_dict = csv.DictReader
        :return: raw = binary, dict = OrderedDict; else None
        """
        file = requests.get(self.get_query_url_to_file(query_id))

        if type == 'binary':
            return file.content
        elif type == 'dict':
            return csv.DictReader(io.StringIO(file.text))
        else:
            return None

    def delete_query(self, query_id):
        """
        Delete query ID in DBM with its associated reports.
        :param query_id: QueryID in DBM
        """
        return self.client.queries().deletequery(queryId=query_id).execute()


"""
Additional functions for formatting data
"""

def clean_date_value(str):
    """
    Clean date string and return datetime object.
    :param str: YYYY/MM/DD
    :return: datetime object
    """
    try:
        return datetime.strptime(str, '%Y/%m/%d')
    except ValueError:
        try:
            return datetime.strptime(str, '%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(str, '%Y.%m.%d')
            except ValueError:
                try:
                    return datetime.strptime(str, '%Y/%d/%m')
                except ValueError as e:
                    raise ValueError(e)


def clean_currency_value(str):
    """
    Clean money value and return as float.
    :param str
    :return: float
    """
    pattern = re.compile('[^.0-9]+')
    match = re.sub(pattern, '', str)

    return float(match)