import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from dateutil import rrule


def get_secret(key):
    if key in os.environ:
        return os.environ[key]
    else:
        return st.secrets[key]
 

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


MIXPANEL_DATA_PATH = 'mixpanel_data.csv'

def get_mixpanel_signup_data(use_mixpanel_cache):

    # dates
    if os.path.exists(MIXPANEL_DATA_PATH) and use_mixpanel_cache:
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

def do_brex_api_call(path, next_cursor=None) -> Tuple[List, Optional[str]]:
    url = "https://platform.brexapis.com/v2/" + path + ('' if next_cursor is None else f'?cursor={next_cursor}')

    headers = {"Authorization": f"Bearer {get_secret('BREX_API_TOKEN')}"}

    response = requests.get(url, headers=headers)

    data = response.json()
    return data['items'], data['next_cursor'] if 'next_cursor' in data else None


use_brex_transaction_cache = True
BREX_TRANSACTION_DATA_PATH = 'brex_transaction_data.csv'

def get_brex_transaction_data():

    if os.path.exists(BREX_TRANSACTION_DATA_PATH) and use_brex_transaction_cache:
        df = pd.read_csv(BREX_TRANSACTION_DATA_PATH)
    else:
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
        df.to_csv(BREX_TRANSACTION_DATA_PATH, index=False)
    
    return df


use_brex_account_cache = True
BREX_ACCOUNT_DATA_PATH = 'brex_account_data.csv'

def get_brex_account_data():
    if os.path.exists(BREX_ACCOUNT_DATA_PATH) and use_brex_account_cache:
        df = pd.read_csv(BREX_ACCOUNT_DATA_PATH)
    else:
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
            d['burn'] = d['start_balance'] - d['end_balance']

        df = pd.DataFrame(data)
        df.to_csv(BREX_ACCOUNT_DATA_PATH, index=False)

    return df

def get_runway_string(balance: float, burn: float) -> str:
    if burn < 0:
        return f'${round(burn)}, so runway is infinite.'
    return f'${round(burn)}, for a runway of {round(balance / burn / 12)} years.'


def get_is_default_alive(starting_balance: float, revenue: float, expenses: float, revenue_growth_rate: float) -> bool:

    current_balance = starting_balance
    current_revenue = revenue
    while current_balance > 0:
        if (current_revenue > expenses):
            return True

        current_balance -= expenses

        if current_balance < 0:
            return False

        current_balance += current_revenue
        current_revenue =  current_revenue * (1 + revenue_growth_rate)

    return False

st.title('Mito Company Dashboard')

financial_tab, mixpanel_tab = st.tabs(["Financials", "Mixpanel"])

with financial_tab:
    st.header("Brex Data")
    brex_transaction_data = get_brex_transaction_data()
    brex_account_data = get_brex_account_data()

    # We only look at recent revenue to avoid investment
    recent_revenue = brex_transaction_data[(brex_transaction_data['amount'] >= 0) & (brex_transaction_data['initiated_at_date'] >= '2022-10-01')]
    recent_revenue_summed = recent_revenue.groupby('month').sum(numeric_only=True).reset_index().sort_values(by='month', ascending=False)
    st.plotly_chart(px.bar(recent_revenue_summed, x='month', y='amount', title='Recent Revenue'))

    stripe_revenue = brex_transaction_data[(brex_transaction_data['amount'] >= 0) & (brex_transaction_data['description'] == 'STRIPE - TRANSFER')]
    st.plotly_chart(px.bar(stripe_revenue, x='month', y='amount', title='Stripe Revenue'))

    expenses = brex_transaction_data[brex_transaction_data['amount'] < 0].copy()
    expenses['amount'] = expenses['amount'] * -1
    summed_expenses = expenses.groupby('month').sum(numeric_only=True).reset_index()
    st.plotly_chart(px.bar(summed_expenses, x='month', y='amount', title='Expenses'))

    payroll_expenses = brex_transaction_data[(brex_transaction_data['amount'] < 0) & (brex_transaction_data['description'] == 'RIPPLING - PAYROLL')].copy()
    payroll_expenses['amount'] = payroll_expenses['amount'] * -1
    summed_payroll_expenses = payroll_expenses.groupby('month').sum(numeric_only=True).reset_index()
    st.plotly_chart(px.bar(summed_payroll_expenses, x='month', y='amount', title='Payroll Expenses'))

    st.plotly_chart(px.line(brex_account_data, x='start_date', y='start_balance', title='Money in Bank All Time'))

    st.header("Runway")
    st.write('This section calculates a variety of metrics to show our runway. It allows you to configure some assumptions about our finances, and calculates how long we are alive.')
    st.subheader('Runway Assumptions')

    number_months = st.slider('Number of Months to Consider', min_value=1, max_value=6)

    current_yearly_salary = summed_payroll_expenses['amount'].iloc[-1] / 3 * 12
    new_yearly_salary = st.number_input('Yearly Salary (defaults to current)', value=current_yearly_salary, step=1000.0)
    salary_adjustment = (new_yearly_salary - current_yearly_salary) / 12

    revenue_growth_rate = st.number_input('Monthly Revenue Growth (%)', value=.25)

    # Print the current balance
    st.subheader('Runway Calculations')
    balance = brex_account_data['end_balance'].iloc[0]

    # First, calculate all the terms we need below
    min_revenue = recent_revenue_summed['amount'].min()
    max_revenue = recent_revenue_summed['amount'].max()
    avg_revenue = recent_revenue_summed['amount'].mean()
    summed_expenses = expenses.groupby('month').sum(numeric_only=True).reset_index().sort_values(by='month', ascending=False).head(number_months)
    min_gross_burn = summed_expenses['amount'].min() + salary_adjustment
    max_gross_burn = summed_expenses['amount'].max() + salary_adjustment
    avg_gross_burn = summed_expenses['amount'].mean() + salary_adjustment
    min_net_burn = brex_account_data['burn'].head(number_months).min() + salary_adjustment
    max_net_burn = brex_account_data['burn'].head(number_months).max() + salary_adjustment
    avg_net_burn = brex_account_data['burn'].head(number_months).mean() + salary_adjustment


    # First, we calculate default alive under the worst possible assumptions
    worst_case_default_alive = get_is_default_alive(balance, min_revenue, max_gross_burn, revenue_growth_rate)
    avg_case_default_alive = get_is_default_alive(balance, avg_revenue, avg_gross_burn, revenue_growth_rate)
    best_case_default_alive = get_is_default_alive(balance, max_revenue, min_gross_burn, revenue_growth_rate)

    st.text(f'Worst case default alive: {worst_case_default_alive}')
    st.text(f'Average case default alive: {avg_case_default_alive}')
    st.text(f'Best case default alive: {best_case_default_alive}')


    st.text("Balance at end of last statement: {:,}".format(balance))

    # First, calculate for net burn
    st.text(f'Minimum net burn in the last {number_months} months: {get_runway_string(balance, min_net_burn)}')
    st.text(f'Maxiumum net burn in the last {number_months} months: {get_runway_string(balance, max_net_burn)}')
    st.text(f'Average net burn in the last {number_months} months: {get_runway_string(balance, avg_net_burn)}')

    # Then, calculate for just expenses
    st.text(f'Minimum gross burn in the last {number_months} months: {get_runway_string(balance, min_gross_burn)}')
    st.text(f'Maxiumum gross burn in the last {number_months} months: {get_runway_string(balance, max_gross_burn)}')
    st.text(f'Average gross burn in the last {number_months} months: {get_runway_string(balance, avg_gross_burn)}')

    

with mixpanel_tab:
    st.header("Mixpanel Data")
    do_refresh = st.button('Refresh Cache')
    mixpanel_signup_data = get_mixpanel_signup_data(use_mixpanel_cache=not do_refresh)

    # Mixpanel things
    st.plotly_chart(px.line(mixpanel_signup_data, x='Month', y='Num Installs', title='Num Installs'))
    st.plotly_chart(px.line(mixpanel_signup_data, x='Month', y='Num Signups', title='Num Finished Signups'))
    st.plotly_chart(px.line(mixpanel_signup_data, x='Month', y='Install Success Rate', title='Install Success Rate'))

