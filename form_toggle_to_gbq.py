import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import asana
import time
from google.cloud import bigquery
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

client = bigquery.Client.from_service_account_json(
    'secret-file.json')


# Get data from Toggle
def report(api_key, workspace_id):
    global toggl_report

    previous_day = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    toggl_report = pd.DataFrame()

    params = (
        ('workspace_id', workspace_id),
        ('since', previous_day),
        ('until', previous_day),
        ('user_agent', 'api_test'),
    )

    response = requests.get('https://api.track.toggl.com/reports/api/v2/details',
                            params=params, auth=(api_key, 'api_token'))

    obj = []
    for data in response.json()['data']:
        obj.append({'date': data['end'],
                    'task': data['description'],
                    'time': data['dur']})

    report_df = pd.DataFrame(obj)
    report_df['time'] = report_df['time'] / 60000
    report_df[['date']] = pd.to_datetime(report_df[['date']].stack()).unstack()
    report_df['date'] = report_df['date'].dt.date

    toggl_report = toggl_report.append(report_df, ignore_index=True)

# Run func for each team member
report('e9debf45a726f07660044beb89dtest', '5038666') # api_key and workspace_id
toggl_report_oleg = toggl_report.copy()
toggl_report_oleg['name'] = 'Олег Гонштейн'
time.sleep(5)

report('6b59b408a84093602d94287066test', '5665666')
toggl_report_ivan = toggl_report.copy()
toggl_report_ivan['name'] = 'Иван Иванов'
time.sleep(5)

toggl_report_all = pd.concat([toggl_report_oleg, toggl_report_ivan])

toggl_report_all['task'] = toggl_report_all['task'].str.strip()

toggl_report_all = toggl_report_all.groupby(['date', 'name', 'task']
                                           ).agg({'time':'sum'}).reset_index()

toggl_report_all['time'] = toggl_report_all['time'].round()

# Load data to BigQuery
df_dict = toggl_report_all.to_dict('r')
table = client.get_table('project-name.dataset-name.table-name') # insert your path to table
rows_to_insert = df_dict
client.insert_rows(table, rows_to_insert)
