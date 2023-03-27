import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
import plotly.express as px
import requests
import snowflake.connector
import streamlit as st
from dateutil import rrule

logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('stripe').setLevel(logging.WARNING)


def get_secret(key):
    import os

    import streamlit as st
    
    if key in os.environ:
        return os.environ[key]
    else:
        return st.secrets[key]

def get_snowflake_table_as_df(schema: str, table: str) -> pd.DataFrame:
    con = snowflake.connector.connect(
        user=get_secret('SNOWFLAKE_USERNAME'), 
        password=get_secret('SNOWFLAKE_PASSWORD'), 
        account=get_secret('SNOWFLAKE_ACCOUNT'), 
        warehouse='COMPUTE_WH', 
        database='DASHBOARD_DATA', 
        schema=schema
    )

    cur = con.cursor()
    cur.execute(f'SELECT * FROM {table}')
    df = cur.fetch_pandas_all()
    df.columns = [col.lower() for col in df.columns]
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

def get_stripe_subscriptions_at_time(stripe_subscriptions: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    return stripe_subscriptions[(stripe_subscriptions['start_date'] < dt) & ((stripe_subscriptions['end_date'].isna()) | (stripe_subscriptions['end_date'] >= dt))]

def get_team_customers_at_time(team_customer_data: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    return team_customer_data[(team_customer_data['start_date'] < dt) & (team_customer_data['end_date'] >= dt)]

def get_revenue_and_customers_dataframe(stripe_subscriptions: pd.DataFrame, team_customer_data: pd.DataFrame, mrr_or_arr: Literal['MRR', 'ARR']) -> pd.DataFrame:    
    start_date = (datetime.now() - timedelta(weeks=30)).replace(day=1)
    end_date = datetime.now().replace(day=1)

    times = []
    stripe_revenues = []
    stripe_subscription_count = []
    teams_revenue = []
    teams_subscription_count = []
    total_revenue = []

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        
        stripe_subs = get_stripe_subscriptions_at_time(stripe_subscriptions, dt)
        team_subs = get_team_customers_at_time(team_customer_data, dt)

        times.append(dt)
        stripe_revenues.append(stripe_subs['amount'].sum())
        stripe_subscription_count.append(len(stripe_subs))
        teams_revenue.append(team_subs['monthly_amount'].sum())
        teams_subscription_count.append(len(team_subs))
        total_revenue.append(stripe_revenues[-1] + teams_revenue[-1])

    # We then append one more for the current date, so we can see the current revenue
    today = datetime.now()
    if times[-1] != today:
        stripe_subs = get_stripe_subscriptions_at_time(stripe_subscriptions, today)
        team_subs = get_team_customers_at_time(team_customer_data, today)

        times.append(today)
        stripe_revenues.append(stripe_subs['amount'].sum())
        stripe_subscription_count.append(len(stripe_subs))
        teams_revenue.append(team_subs['monthly_amount'].sum())
        teams_subscription_count.append(len(team_subs))
        total_revenue.append(stripe_revenues[-1] + teams_revenue[-1])

    if mrr_or_arr == 'ARR':
        stripe_revenues = list(map(lambda x: x * 12, stripe_revenues))
        teams_revenue = list(map(lambda x: x * 12, teams_revenue))
        total_revenue = list(map(lambda x: x * 12, total_revenue))

    return pd.DataFrame({
        'time': times,
        'stripe_revenue': stripe_revenues,
        'stripe_subscription_count': stripe_subscription_count,
        'team_revenue': teams_revenue,
        'team_subscription_count': teams_subscription_count,
        'total_revenue': total_revenue
    })


def get_retention_at_x_months(retention_data: pd.DataFrame, month_x: int) -> Dict[datetime, float]:
    retention_in_month = {}
    for _, row in retention_data.iterrows():
        start_date: datetime = row['start_date']
        initial_size = row['initial_size']

        month_x_date = start_date
        for m in range(month_x):
            month_x_date += timedelta(days=32)
            month_x_date = month_x_date.replace(day=1)

        key = f'period_{month_x_date.strftime("%Y_%m_%d")}'
        if key in row:
            num_in_month = row[key]
            retention_in_month[start_date] = num_in_month / initial_size

    return retention_in_month

def get_retention_dict(retention_data: pd.DataFrame, last_n_months) -> pd.DataFrame:
    n_months_ago = datetime.now() - timedelta(days=last_n_months*30)
    values = []
    for month_x in range(len(retention_data)):
        retention_in_month = get_retention_at_x_months(retention_data, month_x)
        for start_date, percent in retention_in_month.items():
            if start_date > n_months_ago:
                values.append((month_x, start_date.strftime("%Y_%m_%d"), percent))

    return pd.DataFrame(values, columns=['Month X', 'Start Date', 'Percentage'])
    
def convert_notion_property_to_raw_value(value: Any) -> Union[str, int, float, bool]:
    if not isinstance(value, dict) and np.isnan(value):
        return np.NaN
    elif isinstance(value, dict):
        if value['type'] == 'title':
            title_list = value['title']
            if len(title_list) == 0:
                return ''
            else:
                return title_list[0]['text']['content']
        if value['type'] == 'rich_text':
            final_str = ''
            for t in value['rich_text']:
                final_str += t['plain_text']

            return final_str
        elif value['type'] == 'number':
            return value['number']
        elif value['type'] == 'multi_select':
            selected = []
            for ms in value['multi_select']:
                selected.append(ms['name'])
            return ", ".join(selected)
        elif value['type'] == 'select':
            return value['select']['name']
        elif value['type'] == 'date':
            return pd.to_datetime(value['date']['start']).date()
        elif value['type'] == 'people':
            people_list = value['people']
            if len(people_list) == 0:
                return ''
            else:
                return people_list[0]['name']

    return str(value)

def convert_column_of_notion_properties_to_raw_values(column: pd.Series) -> pd.Series:
    return column.apply(convert_notion_property_to_raw_value)
    

def get_notion_database(database_id: str, properties_only=True) -> pd.DataFrame:

    headers = {
        "Authorization": "Bearer " + get_secret('NOTION_API_KEY'),
        "Content-Type": "application/json",
        "Notion-Version": "2021-05-13"
    }

    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    res = requests.request("POST", url, headers=headers)
    data = res.json()

    results = pd.DataFrame(data['results'])
    if properties_only:
        properties_df = pd.DataFrame(list(results['properties']))
        return properties_df.apply(lambda col: convert_column_of_notion_properties_to_raw_values(col), axis=1)
    else:
        return results


st.title('Mito Company Dashboard')

revenue_tab, expense_tab, mixpanel_tab, website_traffic_tab, growth_tab, sales_tab, support_tab = st.tabs(["Revenue", "Expenses", "Mixpanel", "Website Traffic", "Growth", "Sales", "Support"])

with revenue_tab:
    brex_transaction_data = get_snowflake_table_as_df('BREX', 'TRANSACTION_DATA')
    brex_account_data = get_snowflake_table_as_df('BREX', 'ACCOUNT_DATA')
    team_customer_data = get_snowflake_table_as_df('TEAMS', 'CUSTOMERS')
    stripe_subscriptions = get_snowflake_table_as_df('STRIPE', 'SUBSCRIPTIONS')

    st.header('Revenue')

    mrr_or_arr: Literal['MRR', 'ARR'] = st.selectbox('MRR or ARR', ['MRR', 'ARR']) # type: ignore
    revenue_per_month = get_revenue_and_customers_dataframe(stripe_subscriptions, team_customer_data, mrr_or_arr)
    st.plotly_chart(
        px.bar(
            revenue_per_month, x='time', y=['stripe_revenue', 'team_revenue', 'total_revenue'], 
            barmode="group",
            title=f"Revenue ({mrr_or_arr})", labels={'value': mrr_or_arr, 'variable': ''},
        )
    )

    # We only look at recent revenue to avoid investment
    recent_revenue = brex_transaction_data[(brex_transaction_data['amount'] >= 0) & (brex_transaction_data['initiated_at_date'] >= pd.to_datetime('2022-10-01'))]
    recent_revenue_summed = recent_revenue.groupby('month').sum(numeric_only=True).reset_index().sort_values(by='month', ascending=False)
    st.plotly_chart(px.bar(recent_revenue_summed, x='month', y='amount', title='Actual Income (Money Entering Bank Account)'))

    st.header("Revenue Breakdown")

    current_team_customers = team_customer_data[team_customer_data['end_date'] >= datetime.now()]
    current_teams_revenue = current_team_customers['monthly_amount'].sum()

    st.subheader("Revenue and Subscribers Over Time")
    revenue_per_month = get_revenue_and_customers_dataframe(stripe_subscriptions, team_customer_data, 'MRR')
    st.dataframe(revenue_per_month)
    
    st.subheader('Current Teams')
    st.dataframe(current_team_customers)

    st.subheader('Revenue from Stripe')
    stripe_revenue = brex_transaction_data[(brex_transaction_data['amount'] >= 0) & (brex_transaction_data['description'] == 'STRIPE - TRANSFER')]
    st.plotly_chart(px.bar(stripe_revenue, x='month', y='amount', title='Revenue from Stripe'))


with expense_tab:
    expenses = brex_transaction_data[brex_transaction_data['amount'] < 0].copy()
    expenses['amount'] = expenses['amount'] * -1
    summed_expenses = expenses.groupby('month').sum(numeric_only=True).reset_index()
    st.plotly_chart(px.bar(summed_expenses, x='month', y='amount', title='Expenses'))

    payroll_expenses = brex_transaction_data[(brex_transaction_data['amount'] < 0) & (brex_transaction_data['description'].str.contains('RIPPLING'))].copy()
    payroll_expenses['amount'] = payroll_expenses['amount'] * -1
    summed_payroll_expenses = payroll_expenses.groupby('month').sum(numeric_only=True).reset_index()
    st.plotly_chart(px.bar(summed_payroll_expenses, x='month', y='amount', title='Payroll Expenses'))

    st.plotly_chart(px.line(brex_account_data, x='start_date', y='start_balance', title='Money in Bank All Time'))

    st.header("Runway")
    st.write('This section calculates a variety of metrics to show our runway. It allows you to configure some assumptions about our finances, and calculates how long we are alive.')
    st.subheader('Runway Assumptions')

    number_months = st.slider('Number of Months to Consider', min_value=1, max_value=12)

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
    mixpanel_signup_data = get_snowflake_table_as_df('MIXPANEL', 'SIGNUPS')
    mixpanel_retention_data = get_snowflake_table_as_df('MIXPANEL', 'RETENTION')
    
    # Mixpanel things
    st.subheader('Signup Data')
    st.plotly_chart(px.line(mixpanel_signup_data, x='month', y='num_installs', title='Num Installs'))
    st.plotly_chart(px.line(mixpanel_signup_data, x='month', y='num_signups', title='Num Finished Signups'))
    st.plotly_chart(px.line(mixpanel_signup_data, x='month', y='install_success_rate', title='Install Success Rate'))

    st.subheader('Retention Data')
    last_n_months = st.slider('Last N Months:', min_value=1, max_value=len(mixpanel_retention_data))
    retention_data = get_retention_dict(mixpanel_retention_data, last_n_months)
    st.plotly_chart(px.line(retention_data, x='Month X', y='Percentage', color='Start Date', title='Retention'))
    during_month = st.slider('Raw Retention Data During Month:', min_value=1, max_value=len(mixpanel_retention_data))
    all_retention_data = get_retention_dict(mixpanel_retention_data, len(mixpanel_retention_data))
    st.dataframe(all_retention_data[all_retention_data['Month X'] == during_month])

with website_traffic_tab:
    st.header('Website Traffic')

    st.components.v1.iframe(get_secret('PLAUSIBLE_TRYMITO_DASHBOARD'), height=2800)
    st.components.v1.iframe(get_secret('PLAUSIBLE_TRYMITO_BLOG_DASHBOARD'), height=2800)


with growth_tab:

    partnered_content = get_notion_database('5d5c87d7503b47a3a9622957d6ac7918')
    blog_promotion_content = get_notion_database('ff34057e55c842799b71f775d105c701')

    # Allow the users to see growth tasks in a specific range
    today = datetime.today()
    one_week_ago = today - timedelta(days=7)
    min_date, max_date = st.date_input('Growth Tasks in Date', value=(one_week_ago, today))

    st.header(f'Growth Work between {min_date}-{max_date}')
    range_partnered_content = partnered_content[(partnered_content['Date'] >= min_date) & (partnered_content['Date'] <= max_date)]
    range_blog_promotion_content = blog_promotion_content[(blog_promotion_content['Date'] >= min_date) & (blog_promotion_content['Date'] <= max_date)]
    st.subheader(f'Partnered Content between {min_date}-{max_date}')
    st.dataframe(range_partnered_content)
    st.subheader(f'Blog Content Promotion between {min_date}-{max_date}')
    st.dataframe(range_blog_promotion_content)

    st.header('All Growth Trackers')
    st.subheader('Partnered Content')
    st.dataframe(partnered_content)
    st.subheader('Blog Content Promotion')
    st.dataframe(blog_promotion_content)

with sales_tab:

    outreach_tracker = get_notion_database('39d86e3f7e374c8da71e8285df26d955')

    # Allow the users to see growth tasks in a specific range
    today = datetime.today()
    one_week_ago = today - timedelta(days=7)
    min_date, max_date = st.date_input('Outreach Tasks in Date', value=(one_week_ago, today))

    st.header(f'Outreach between {min_date}-{max_date}')
    st.subheader(f'Outreach between {min_date}-{max_date}')
    range_outreach_tracker = outreach_tracker[(outreach_tracker['Date'] >= min_date) & (outreach_tracker['Date'] <= max_date)]
    st.dataframe(range_outreach_tracker)

    st.header('All Outreach')
    st.dataframe(outreach_tracker)

with support_tab:

    support_tracker = get_notion_database('e68d246aca5c4262b1df7095ccecb78e')

     # Allow the users to see growth tasks in a specific range
    today = datetime.today()
    one_week_ago = today - timedelta(days=7)
    min_date, max_date = st.date_input('Support Tasks in Date', value=(one_week_ago, today))

    st.header(f'Support between {min_date}-{max_date}')
    st.subheader(f'Support between {min_date}-{max_date}')
    range_support_tracker = support_tracker[(support_tracker['Date'] >= min_date) & (support_tracker['Date'] <= max_date)]
    st.dataframe(range_support_tracker)

    st.header('All Support')
    st.dataframe(support_tracker)