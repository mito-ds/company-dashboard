import os

import numpy as np
import pandas as pd
import requests
import streamlit as st
import json
from typing import Any, Dict, Tuple
from datetime import datetime, timedelta
from datetime import datetime
from dateutil import rrule
 

def get_mixpanel_query(payload: str, headers: Dict[str, str]) -> Dict[str, Any]:
    service_account_username = os.environ['MIXPANEL_SERVICE_ACCOUNT_USERNAME']
    service_account_password = os.environ['MIXPANEL_SERVICE_ACCOUNT_PASSWORD']

    url = "https://mixpanel.com/api/2.0/engage?project_id=" + os.environ['MIXPANEL_PROJECT_ID']

    response = requests.post(url, data=payload, headers=headers, auth=(service_account_username, service_account_password))
    response = json.loads(response.text)
    return response


def get_mixpanel_paylod_for_first_seen(start_date_str='2022-11-01', end_date_str='2022-11-30') -> str:
    payload = f"filter_by_cohort=%7B%22raw_cohort%22%3A%7B%22name%22%3A%22%22%2C%22id%22%3Anull%2C%22unsavedId%22%3Anull%2C%22groups%22%3A%5B%7B%22type%22%3A%22cohort_group%22%2C%22event%22%3A%7B%22resourceType%22%3A%22cohort%22%2C%22value%22%3A%22%24all_users%22%2C%22label%22%3A%22All%20Users%22%7D%2C%22filters%22%3A%5B%7B%22resourceType%22%3A%22user%22%2C%22propertyName%22%3A%22%24mp_first_event_time%22%2C%22propertyObjectKey%22%3Anull%2C%22propertyDefaultType%22%3A%22datetime%22%2C%22propertyType%22%3A%22datetime%22%2C%22filterOperator%22%3A%22between%22%2C%22filterValue%22%3A%7B%22type%22%3A%22between%22%2C%22from%22%3A%22{start_date_str}%22%2C%22to%22%3A%22{end_date_str}%22%7D%7D%5D%2C%22filtersOperator%22%3A%22and%22%2C%22behavioralFiltersOperator%22%3A%22and%22%2C%22groupingOperator%22%3Anull%2C%22property%22%3Anull%7D%5D%7D%7D"
    return payload


def get_first_seen_and_signed_up_per_month(start_date: datetime, end_date: datetime) -> Tuple[int, int]:

    # Get those users first seen during a time period
    payload = 'output_properties=%5B%22%24email%22%5D&' + get_mixpanel_paylod_for_first_seen(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")) 
    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded"
    }

    response = get_mixpanel_query(payload, headers)
    total_count = response['total']
    page = response['page']
    session_id = response['session_id']

    profiles = response['results']

    while len(profiles) < total_count:
        page = page + 1
        new_payload = payload + f'&session_id={session_id}' + f'&page={page}'
        response = get_mixpanel_query(new_payload, headers)

        profiles += response['results']

    # TODO: filter out fake emails, n@g.com
    with_emails = list(filter(lambda x: '$email' in x['$properties'], profiles))

    return len(with_emails), len(profiles) 


USE_MIXPANEL_CACHE = True
MIXPANEL_DATA_PATH = 'mixpanel_data.csv'

def get_mixpanel_data():

    # dates
    if os.path.exists(MIXPANEL_DATA_PATH) and USE_MIXPANEL_CACHE:
        df = pd.read_csv(MIXPANEL_DATA_PATH)
    else:
        print("Pulling from Mixpanel")
        end_date = datetime.now()
        end_date = (end_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        
        arr = []
        for start_date, end_date in zip(rrule.rrule(rrule.MONTHLY, dtstart=datetime(2022, 1, 1), until=end_date), rrule.rrule(rrule.MONTHLY, dtstart=datetime(2022, 2, 1), until=end_date)):
            #print(start_date, end_date)
            num_signups, num_installs = get_first_seen_and_signed_up_per_month(start_date, end_date)
            arr.append((start_date, num_signups, num_installs, num_signups / num_installs))

        df = pd.DataFrame(arr, columns=['Month', 'Num Signups', 'Num Installs', 'Install Success Rate'])
        df.to_csv(MIXPANEL_DATA_PATH, index=False)

    return df

st.title('Mito Company Dashboard')


mixpanel_data = get_mixpanel_data()

st.subheader('Installs')
st.line_chart(mixpanel_data, x='Month', y='Num Installs')
st.subheader('Finished Signup')
st.line_chart(mixpanel_data, x='Month', y='Num Signups')
st.subheader('Install Success Rate')
st.line_chart(mixpanel_data, x='Month', y='Install Success Rate')


