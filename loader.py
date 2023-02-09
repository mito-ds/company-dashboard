# This file loads things and puts them in snowflake


import datetime
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from datetime import timedelta
from dateutil import rrule
import csv
import pandas as pd
import requests
import snowflake.connector
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('stripe').setLevel(logging.WARNING)

def get_secret(key):
    if key in os.environ:
        return os.environ[key]


def get_value_for_sql_string(val: Any) -> str:
    if isinstance(val, int) or isinstance(val, float):
        return str(val)
    elif isinstance(val, str):
        return f'"{val}"'
    return str(val)

def write_df_to_snowflake(df: pd.DataFrame, warehouse: str, database: str, schema: str, table: str, clear_table=False):

    print(df.columns)

    # Gets the version
    conn = snowflake.connector.connect(
        user=get_secret('SNOWFLAKE_USERNAME'),
        password=get_secret('SNOWFLAKE_PASSWORD'),
        account=get_secret('SNOWFLAKE_ACCOUNT'),
        warehouse=warehouse, 
        database=database, 
        schema=schema
    )

    df.to_csv('to_write.csv', index=False, header=False, quoting=csv.QUOTE_NONNUMERIC)

    conn.cursor().execute(f"PUT file://to_write.csv @~/staged OVERWRITE=TRUE")
    if clear_table:
        conn.cursor().execute(f'DELETE FROM {table}')
    conn.cursor().execute(f"COPY INTO {table} FROM @~/staged/to_write.csv FILE_FORMAT=(FORMAT_NAME=CSVWITHOPTIONALQUOTE)")

    os.remove('to_write.csv')

def do_brex_api_call(path, next_cursor=None) -> Tuple[List, Optional[str]]:
    url = "https://platform.brexapis.com/v2/" + path + ('' if next_cursor is None else f'?cursor={next_cursor}')

    headers = {"Authorization": f"Bearer {get_secret('BREX_API_TOKEN')}"}

    response = requests.get(url, headers=headers)

    data = response.json()
    return data['items'], data['next_cursor'] if 'next_cursor' in data else None

def get_brex_transaction_data():

    path = 'transactions/cash/' + get_secret('BREX_CASH_ACCOUNT_ID')
    data, cursor = do_brex_api_call(path)
    while cursor is not None:
        new_data, cursor = do_brex_api_call(path, cursor)
        data.extend(new_data)

    # We go through and parse our amount and currency
    for d in data:
        d['currency'] = d['amount']['currency']
        d['amount'] = float(d['amount']['amount']) / 100
        d['month'] = pd.to_datetime(d['posted_at_date']).replace(day=1)

    df = pd.DataFrame(data)    

    # Then, write it to snowflake
    
    return df


def get_brex_account_data():
    path = "accounts/cash/" + get_secret('BREX_CASH_ACCOUNT_ID') + "/statements"
    data, cursor = do_brex_api_call(path)
    while cursor is not None:
        new_data, cursor = do_brex_api_call(path, cursor)
        data.extend(new_data)

    for d in data:
        d['start_date'] = d['period']['start_date']
        d['end_date'] = d['period']['end_date']
        d['start_balance'] = float(d['start_balance']['amount']) / 100
        d['end_balance'] = float(d['end_balance']['amount']) / 100
        d['burn'] = round(d['start_balance'] - d['end_balance'])

    df = pd.DataFrame(data)
    df = df.drop(['period'], axis=1)

    # Then, write to snowflake

    return df

def get_stripe_subscriptions():
    import stripe
    stripe.api_key = get_secret('STRIPE_KEY')

    # We list all subscripts all time
    subscriptions = []
    current = stripe.Subscription.list(status='all')['data']
    while len(current) > 0:
        subscriptions.extend(current)
        current = stripe.Subscription.list(status='all', limit=100, starting_after=subscriptions[-1]['id'])['data']

    start_date = []
    end_date = []
    price = []
    default_end_date = datetime.datetime.now() + datetime.timedelta(weeks=(52 * 100)) # 100 years from now
    for subscription in subscriptions:
        start_date.append(datetime.datetime.fromtimestamp(subscription['start_date']))
        # We add the max date onto the end date, as it makes ignoring nulls easier
        end_date.append(datetime.datetime.fromtimestamp(subscription['ended_at']) if subscription['ended_at'] is not None else default_end_date)
        price.append(subscription['plan']['amount'] / 100)

    df = pd.DataFrame({'start_date': start_date, 'end_date': end_date, 'amount': price})
    return df



def get_mixpanel_query(payload: str, headers: Dict[str, str]) -> Dict[str, Any]:
    service_account_username = get_secret('MIXPANEL_SERVICE_ACCOUNT_USERNAME')
    service_account_password = get_secret('MIXPANEL_SERVICE_ACCOUNT_PASSWORD')

    url = "https://mixpanel.com/api/2.0/engage?project_id=" + get_secret('MIXPANEL_PROJECT_ID')

    response = requests.post(url, data=payload, headers=headers, auth=(service_account_username, service_account_password))
    response = json.loads(response.text)
    return response


def get_mixpanel_paylod_for_first_seen(start_date_str='2022-11-01', end_date_str='2022-11-30') -> str:
    payload = f"filter_by_cohort=%7B%22raw_cohort%22%3A%7B%22name%22%3A%22%22%2C%22id%22%3Anull%2C%22unsavedId%22%3Anull%2C%22groups%22%3A%5B%7B%22type%22%3A%22cohort_group%22%2C%22event%22%3A%7B%22resourceType%22%3A%22cohort%22%2C%22value%22%3A%22%24all_users%22%2C%22label%22%3A%22All%20Users%22%7D%2C%22filters%22%3A%5B%7B%22resourceType%22%3A%22user%22%2C%22propertyName%22%3A%22%24mp_first_event_time%22%2C%22propertyObjectKey%22%3Anull%2C%22propertyDefaultType%22%3A%22datetime%22%2C%22propertyType%22%3A%22datetime%22%2C%22filterOperator%22%3A%22between%22%2C%22filterValue%22%3A%7B%22type%22%3A%22between%22%2C%22from%22%3A%22{start_date_str}%22%2C%22to%22%3A%22{end_date_str}%22%7D%7D%5D%2C%22filtersOperator%22%3A%22and%22%2C%22behavioralFiltersOperator%22%3A%22and%22%2C%22groupingOperator%22%3Anull%2C%22property%22%3Anull%7D%5D%7D%7D"
    return payload


def get_first_seen_and_signed_up_per_month(start_date: datetime.datetime, end_date: datetime.datetime) -> Tuple[int, int]:

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

def get_mixpanel_signup_data():
    end_date = datetime.datetime.now()
    end_date = (end_date.replace(day=1) + timedelta(days=32)).replace(day=1)
    
    arr = []
    for start_date, end_date in zip(rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2022, 1, 1), until=end_date), rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2022, 2, 1), until=end_date)):
        #print(start_date, end_date)
        num_signups, num_installs = get_first_seen_and_signed_up_per_month(start_date, end_date)
        arr.append((start_date, num_signups, num_installs, num_signups / num_installs))

    df = pd.DataFrame(arr, columns=['Month', 'Num Signups', 'Num Installs', 'Install Success Rate'])
    return df

if __name__ == '__main__':
    #account_data = get_brex_account_data()
    #write_df_to_snowflake(account_data, 'COMPUTE_WH', 'DASHBOARD_DATA', 'BREX', 'ACCOUNT_DATA', clear_table=True)
    #tx_data = get_brex_transaction_data()
    #write_df_to_snowflake(tx_data, 'COMPUTE_WH', 'DASHBOARD_DATA', 'BREX', 'TRANSACTION_DATA', clear_table=True)
    #stripe_subscriptions = get_stripe_subscriptions()
    #write_df_to_snowflake(stripe_subscriptions, 'COMPUTE_WH', 'DASHBOARD_DATA', 'STRIPE', 'SUBSCRIPTIONS', clear_table=True)
    mixpanel_signups = get_mixpanel_signup_data()
    write_df_to_snowflake(mixpanel_signups, 'COMPUTE_WH', 'DASHBOARD_DATA', 'MIXPANEL', 'SIGNUPS', clear_table=True)


