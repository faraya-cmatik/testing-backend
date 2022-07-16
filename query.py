from google.cloud import bigquery
#from google.oauth2 import service_account
import os
from helper import Helper


class Query:
    def __init__(self):
        #LOCAL
        #self.credentials = service_account.Credentials.from_service_account_file('/Users/mrebolledo/Documents/auth-development-dev.json')
        #self.project = 'cmatik-dev-resources'
        #self.client = bigquery.Client(credentials=self.credentials, project=self.project)
        #CF
        self.client = bigquery.Client()
        self.helper = Helper()

    def get_rows(self,sensors,start_date,end_date):
        return self.get_sensors_data(self.helper.resolve_ids_for_query(sensors),start_date,end_date)

    def get_sensors_data(self,sensors,start_date,end_date):
        query = self.client.query(f"""
                SELECT 
                    sensor_id,
                    result,
                    date
                FROM 
                    {os.environ.get('BQ_DATASET')}.{os.environ.get('BQ_TABLE')}
                WHERE
                    sensor_id IN {sensors}
                    AND date BETWEEN '{start_date}' AND '{end_date}'   
                ORDER BY date ASC      
            """)
        return query.result()    