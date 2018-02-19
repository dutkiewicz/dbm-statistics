from datetime import datetime
from httplib2 import Http
import json
import re

from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build


class DBMQuery():
    """
    Class for making queries to API
    """

    def __init__(self, auth_json):
        """
        prepare OAuth2 credentials for service-to-service API calls
        """
        self.auth_json = auth_json
        self.scope = ['https://www.googleapis.com/auth/doubleclickbidmanager']

    def authorize(self):
        """
        authorize with Google's OAuth2 and build our API object
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.auth_json, self.scope)
        http_auth = credentials.authorize(Http())

        return build('doubleclickbidmanager', 'v1', http=http_auth)


def run_query(query_id, client):
    """
    Run pre-compiled query in DBM.
    :param query_id: QueryID in DBM
    :param client: DBM authorized client
    :return: json response from DBM
    """
    json_body = """
                    {
                      "dataRange": "YESTERDAY",
                      "timezoneCode": "Europe/Warsaw"
                    }
                    """
    body = json.loads(json_body)
    return client.queries().runquery(queryId=query_id, body=body).execute()


def list_queries(client):
    """
    List available created queries in DBM
    :param client: QueryID in DBM
    :return:
    """
    query = client.queries().listqueries().execute()

    try:
        print("Query ID", "Name", "Last run date", "Is running?", sep=" | ")
        for q in query['queries']:
            print(q['queryId'],
                  q['metadata']['title'],
                  datetime.fromtimestamp(int(q['metadata']['latestReportRunTimeMs']) / 1000.0).strftime(
                      '%Y-%m-%d %H:%M:%S'),
                  q['metadata']['running'],
                  sep=" | ")
    except KeyError:
        print("No queries are created for this account. Use -c to create new one.")
        exit(0)

def create_query(file, client):
    """
    Create new query from json.
    :param body: json with query parameters
    :param client: DBM authorized client
    :return:
    """
    try:
        with open(file) as file:
            body = json.load(file)
    except AttributeError:
        raise AttributeError("{} is not a file, please specify path to json file".format(body))
    except json.decoder.JSONDecodeError:
        raise TypeError("{} is not a json file".format(body))

    return client.queries().createquery(body=body).execute()


def download_query(query_id, client):
    """
    Returns URL to existing report in DBM
    :param query_id: QueryID in DBM
    :param client: DBM authorized client
    :return:
    """
    query = client.queries().getquery(queryId=query_id).execute()

    if not query:
        raise ValueError("Query ID {} does not exist in DBM. Have you run query before downloading it?".format(query_id))
    elif query['metadata']['running']:
        raise RuntimeWarning("Query ID {} is still running!".format(query_id))
    else:
        return query['metadata']['googleCloudStoragePathForLatestReport']


def delete_query(query_id, client):
    """
    Delete query ID in DBM with its associated reports.
    :param query_id: QueryID in DBM
    :param client: DBM authorized client
    """
    return client.queries().deletequery(queryId=query_id).execute()


def clean_date_value(str):
    """
    Clean date string and return datetime object.
    :param str: YYYY/MM/DD
    :return: datetime object
    """
    return datetime.strptime(str, '%Y/%m/%d')

def clean_money_value(str):
    """
    Clean money value and return as float.
    :param str
    :return: float
    """
    match = "([a-zA-Z])|([,])"
    return float(re.sub(match, "", str))