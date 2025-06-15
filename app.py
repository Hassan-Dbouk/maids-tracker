import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import os

# Set Google Credentials (only if running locally)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# Initialize BigQuery client
client = bigquery.Client()

# --- Loaders ---
@st.cache_data(ttl=3600)
def load_applications():
    query = """
    SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
        `Nationality Category Updated` AS nationality,
        `Location Category Updated` AS location,
        `Activa Visa Status` AS active_visa_status
    FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
    WHERE `Application Created` IS NOT NULL
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_quotas():
    query = """
    SELECT
        `Nationality Category Updated` AS nationality,
        `Location Category Updated` AS location,
        `Daily Quota Regardless Active Visas` AS quota_all,
        `Daily Quota Considering Active Visas` AS quota_active
    FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
    """
    return client.query(query).to_dataframe()

# --- Filters ---
def filter_applications(df, nat, loc, active_only):
    df = df[(df["nationality"] == nat) & (df["location"] == loc)]
    if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
        df = df[df["active_visa_status"] == "true"]
    return df

def get_daily_quota(df, nat, loc, active_only):
    row = df[(df["nationality"] == nat) & (df["location"] == loc)]
    if row.empty:
        return 0
    return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# --- Aggregation ---
def prepare_grouped(df, level, this_year, last_year, today):
    df = df.copy()
    df["application_date"] = pd.to_datetime(df["application_date"])
    df["year"] = df["application_date"].dt.year
    df = df[df["year"].isin([this_year, last_year])]

    if level == "M":
        df["period"] = df["application_date"].dt.strftime('%b')
        period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
    elif level == "W":
        df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
    elif level == "D":
        df["period"] = df["application_date"].dt.strftime('%d/%m')

    grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
    pivoted = grouped.pivot(index="period", columns="year", values="applications")

    if level == "M" and this_year in pivoted.columns:
        latest_month = today.month
        month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        later_months = month_abbrs[latest_month:]
        pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
    elif level == "W" and this_year in pivoted.columns:
        current_week = today.isocalendar().week
        pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
    elif level == "D" and this_year in pivoted.columns:
        df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
        valid_days = df_cutoff["period"].unique()
        invalid_days = [day for day in pivoted.index if day not in valid_days]
        pivoted.loc[invalid_days, this_year] = float('nan')

    if level in ["W", "D"]:
        pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

    return pivoted

# --- Plotting ---
def plot_chart(df, title, this_year, last_year, needed_avg, level, today):
    fig = go.Figure()
    if last_year in df.columns:
        fig.add_trace(go.Scatter(x=df.index, y=df[last_year], name=f"{last_year}", line=dict(color='gray', dash='dot'), mode='lines'))
    if this_year in df.columns:
        fig.add_trace(go.Scatter(x=df.index, y=df[this_year], name=f"{this_year}", line=dict(color='green'), mode='lines'))
        future_x = []
        if level == "D":
            tomorrow = today + timedelta(days=1)
            def safe_parse(x):
                try:
                    return datetime.strptime(f"{x}/{this_year}", "%d/%m/%Y")
                except:
                    return None
            future_x = [x for x in df.index if isinstance(x, str) and safe_parse(x) and safe_parse(x) >= tomorrow]
        else:
            future_x = [x for x in df.index if pd.isna(df.loc[x, this_year])]
        if future_x:
            fig.add_trace(go.Scatter(x=future_x, y=[needed_avg]*len(future_x), name="Required Avg", line=dict(color='red', dash='dot'), mode='lines'))

    fig.update_layout(title=title, xaxis_title="", yaxis_title="Applications", hovermode="x unified", showlegend=True)
    if level == "D":
        fig.update_xaxes(tickvals=[x for x in df.index if isinstance(x, str) and x.startswith("01/")])
    fig.update_xaxes(tickangle=0)
    fig.update_yaxes(tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.markdown("<h4>MaidsAT - Tracker</h4>", unsafe_allow_html=True)

# Load data
df_apps = load_applications()
df_quotas = load_quotas()

# Sidebar
st.sidebar.header("🔍 Filters")
nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()), index=sorted(df_apps["nationality"].dropna().unique()).index("filipina"))
loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()), index=sorted(df_apps["location"].dropna().unique()).index("outside_uae"))
active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"], index=1) == "Yes"

# Filtering
df_filtered = filter_applications(df_apps, nat, loc, active_only)
df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
today = pd.to_datetime("today").normalize()
this_year = today.year
last_year = this_year - 1
year_days = 366 if today.year % 4 == 0 else 365
daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
total_quota = daily_quota * year_days

# --- Monthly Forecast Table ---
first_day = today.replace(day=1)
last_day = (first_day + pd.offsets.MonthEnd(0)).date()
days_passed = (today.date() - first_day.date()).days + 1
remaining_days_in_month = (last_day - today.date()).days

monthly_quota = daily_quota * (days_passed + remaining_days_in_month)
attained_this_month = df_filtered[(df_filtered["application_date"] >= first_day) & (df_filtered["application_date"] <= today)].shape[0]
percent_delivered = (attained_this_month / monthly_quota * 100) if monthly_quota else 0
forecast_by_eom = int(attained_this_month / days_passed * (days_passed + remaining_days_in_month)) if days_passed else 0
percent_forecast = (forecast_by_eom / monthly_quota * 100) if monthly_quota else 0

summary_table = pd.DataFrame({
    "Monthly Quota": [f"{monthly_quota:,.0f}"],
    "Delivered": [f"{attained_this_month:,.0f}"],
    "%D": [f"{percent_delivered:.1f}%"],
    "Forecast": [f"{forecast_by_eom:,.0f}"],
    "%F": [f"{percent_forecast:.1f}%"]
})

# Styled KPI Table
st.markdown("""
<style>
    table {
        border-collapse: collapse;
        margin-left: auto;
        margin-right: auto;
        width: 90% !important;
        table-layout: fixed;
    }
    thead th {
        background-color: #cce5ff;
        font-weight: bold;
        text-align: center;
        border: 1px solid black;
    }
    tbody td {
        text-align: center;
        vertical-align: middle;
        border: 1px solid black;
    }
</style>
""", unsafe_allow_html=True)

latest_date = df_filtered['application_date'].max().strftime('%Y-%m-%d')
st.markdown(f"<p style='font-size:15px;'>Latest Day Considered: <strong>{latest_date}</strong></p>", unsafe_allow_html=True)
st.markdown("<p style='font-size:18px; font-weight:bold;'>Monthly KPI Summary</p>", unsafe_allow_html=True)
st.markdown(summary_table.to_html(index=False, escape=False), unsafe_allow_html=True)

# --- Render Views ---
view_levels = [("Monthly View", "M"), ("Weekly View", "W"), ("Daily View", "D")]
for label, level in view_levels:
    df_plot = prepare_grouped(df_filtered, level, this_year, last_year, today)
    attained = df_filtered[df_filtered["application_date"].dt.year == this_year].shape[0]
    remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
    needed_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0
    if level == "M": needed_avg *= 30
    elif level == "W": needed_avg *= 7
    plot_chart(df_plot, label, this_year, last_year, needed_avg, level, today)














# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Plotting ---
# def plot_chart(df, title, this_year, last_year, needed_avg, level, today):
#     fig = go.Figure()
#     if last_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[last_year], name=f"{last_year}", line=dict(color='gray', dash='dot'), mode='lines'))
#     if this_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[this_year], name=f"{this_year}", line=dict(color='green'), mode='lines'))
#         future_x = []
#         if level == "D":
#             tomorrow = today + timedelta(days=1)
#             def safe_parse(x):
#                 try:
#                     return datetime.strptime(f"{x}/{this_year}", "%d/%m/%Y")
#                 except:
#                     return None
#             future_x = [x for x in df.index if isinstance(x, str) and safe_parse(x) and safe_parse(x) >= tomorrow]
#         else:
#             future_x = [x for x in df.index if pd.isna(df.loc[x, this_year])]
#         if future_x:
#             fig.add_trace(go.Scatter(x=future_x, y=[needed_avg]*len(future_x), name="Required Avg", line=dict(color='red', dash='dot'), mode='lines'))

#     fig.update_layout(title=title, xaxis_title="", yaxis_title="Applications", hovermode="x unified", showlegend=True)
#     if level == "D":
#         fig.update_xaxes(tickvals=[x for x in df.index if isinstance(x, str) and x.startswith("01/")])
#     fig.update_xaxes(tickangle=0)
#     fig.update_yaxes(tickformat=",.0f")
#     st.plotly_chart(fig, use_container_width=True)

# # --- Streamlit UI ---
# st.set_page_config(layout="wide")
# st.title("MaidsAT - Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()), index=sorted(df_apps["nationality"].dropna().unique()).index("filipina"))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()), index=sorted(df_apps["location"].dropna().unique()).index("outside_uae"))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"], index=1) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # --- Monthly Forecast Table ---
# first_day = today.replace(day=1)
# last_day = (first_day + pd.offsets.MonthEnd(0)).date()
# days_passed = (today.date() - first_day.date()).days + 1
# remaining_days_in_month = (last_day - today.date()).days

# monthly_quota = daily_quota * (days_passed + remaining_days_in_month)
# attained_this_month = df_filtered[(df_filtered["application_date"] >= first_day) & (df_filtered["application_date"] <= today)].shape[0]
# percent_delivered = (attained_this_month / monthly_quota * 100) if monthly_quota else 0
# forecast_by_eom = int(attained_this_month / days_passed * (days_passed + remaining_days_in_month)) if days_passed else 0
# percent_forecast = (forecast_by_eom / monthly_quota * 100) if monthly_quota else 0

# summary_table = pd.DataFrame({
#     "Monthly Quota": [f"{monthly_quota:,.0f}"],
#     "Delivered": [f"{attained_this_month:,.0f}"],
#     "%D": [f"{percent_delivered:.1f}%"],
#     "Forecast": [f"{forecast_by_eom:,.0f}"],
#     "%F": [f"{percent_forecast:.1f}%"]
# })

# # Styled KPI Table
# st.markdown("""
# <style>
#     table {
#         border-collapse: collapse;
#         margin-left: auto;
#         margin-right: auto;
#         width: 90% !important;
#         table-layout: fixed;
#     }
#     thead th {
#         background-color: #cce5ff;
#         font-weight: bold;
#         text-align: center;
#         border: 1px solid black;
#     }
#     tbody td {
#         text-align: center;
#         vertical-align: middle;
#         border: 1px solid black;
#     }
# </style>
# """, unsafe_allow_html=True)

# st.markdown("<h4 style='text-align:left;'>Monthly KPI Summary</h4>", unsafe_allow_html=True)
# st.markdown(summary_table.to_html(index=False, escape=False), unsafe_allow_html=True)

# # --- Render Views ---
# view_levels = [("Monthly View", "M"), ("Weekly View", "W"), ("Daily View", "D")]
# for label, level in view_levels:
#     df_plot = prepare_grouped(df_filtered, level, this_year, last_year, today)
#     attained = df_filtered[df_filtered["application_date"].dt.year == this_year].shape[0]
#     remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
#     needed_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0
#     if level == "M": needed_avg *= 30
#     elif level == "W": needed_avg *= 7
#     plot_chart(df_plot, label, this_year, last_year, needed_avg, level, today)













# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Plotting ---
# def plot_chart(df, title, this_year, last_year, needed_avg, level):
#     fig = go.Figure()
#     if last_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[last_year], name=f"{last_year}", line=dict(color='gray', dash='dot'), mode='lines'))
#     if this_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[this_year], name=f"{this_year}", line=dict(color='green'), mode='lines'))
#         future_x = [x for x in df.index if pd.isna(df.loc[x, this_year])]
#         if future_x:
#             fig.add_trace(go.Scatter(x=future_x, y=[needed_avg]*len(future_x), name="Required Avg", line=dict(color='red', dash='dot'), mode='lines'))

#     fig.update_layout(title=title, xaxis_title="", yaxis_title="Applications", hovermode="x unified", showlegend=True)
#     if level == "D":
#         fig.update_xaxes(tickvals=[x for x in df.index if isinstance(x, str) and x.startswith("01/")])
#     fig.update_xaxes(tickangle=0)
#     fig.update_yaxes(tickformat=",.0f")
#     st.plotly_chart(fig, use_container_width=True)

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # --- Monthly Forecast Table ---
# first_day = today.replace(day=1)
# last_day = (first_day + pd.offsets.MonthEnd(0)).date()
# days_passed = (today.date() - first_day.date()).days + 1
# remaining_days_in_month = (last_day - today.date()).days

# monthly_quota = daily_quota * (days_passed + remaining_days_in_month)
# attained_this_month = df_filtered[(df_filtered["application_date"] >= first_day) & (df_filtered["application_date"] <= today)].shape[0]
# percent_delivered = (attained_this_month / monthly_quota * 100) if monthly_quota else 0
# forecast_by_eom = int(attained_this_month / days_passed * (days_passed + remaining_days_in_month)) if days_passed else 0
# percent_forecast = (forecast_by_eom / monthly_quota * 100) if monthly_quota else 0

# summary_table = pd.DataFrame({
#     "Monthly Quota": [f"{monthly_quota:,.0f}"],
#     "Delivered": [f"{attained_this_month:,.0f}"],
#     "%D": [f"{percent_delivered:.1f}%"],
#     "Forecast": [f"{forecast_by_eom:,.0f}"],
#     "%F": [f"{percent_forecast:.1f}%"]
# })

# # Styled KPI Table
# st.markdown("""
# <style>
#     table {
#         border-collapse: collapse;
#         margin-left: auto;
#         margin-right: auto;
#     }
#     thead th {
#         background-color: #cce5ff;
#         font-weight: bold;
#         text-align: center;
#         border: 1px solid black;
#     }
#     tbody td {
#         text-align: center;
#         vertical-align: middle;
#         border: 1px solid black;
#     }
# </style>
# """, unsafe_allow_html=True)

# st.markdown("### 📅 Monthly KPI Summary")
# st.markdown(summary_table.to_html(index=False, escape=False), unsafe_allow_html=True)

# # --- Render Views ---
# view_levels = [("Monthly View", "M"), ("Weekly View", "W"), ("Daily View", "D")]
# for label, level in view_levels:
#     df_plot = prepare_grouped(df_filtered, level, this_year, last_year, today)
#     attained = df_filtered[df_filtered["application_date"].dt.year == this_year].shape[0]
#     remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
#     needed_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0
#     if level == "M": needed_avg *= 30
#     elif level == "W": needed_avg *= 7
#     st.markdown(f"## {label}")
#     plot_chart(df_plot, label, this_year, last_year, needed_avg, level)




# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Plotting ---
# def plot_chart(df, title, this_year, last_year, needed_avg, level):
#     fig = go.Figure()
#     if last_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[last_year], name=f"{last_year}", line=dict(color='gray', dash='dot')))
#     if this_year in df.columns:
#         fig.add_trace(go.Scatter(x=df.index, y=df[this_year], name=f"{this_year}", line=dict(color='green')))
#         future_x = [x for x in df.index if pd.isna(df.loc[x, this_year])]
#         if future_x:
#             fig.add_trace(go.Scatter(x=future_x, y=[needed_avg]*len(future_x), name="Required Avg", line=dict(color='red', dash='dot')))

#     fig.update_layout(title=title, xaxis_title="", yaxis_title="Applications", hovermode="x unified")
#     if level == "M":
#         fig.update_yaxes(tickformat=",.0f")
#     elif level == "W":
#         fig.update_xaxes(title="Week #")
#     elif level == "D":
#         fig.update_xaxes(tickvals=[d for d in df.index if isinstance(d, str) and d.startswith(('01/', '15/'))])

#     fig.update_xaxes(tickangle=0)
#     st.plotly_chart(fig, use_container_width=True)

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # --- Monthly Forecast Table ---
# first_day = today.replace(day=1)
# last_day = (first_day + pd.offsets.MonthEnd(0)).date()
# days_passed = (today.date() - first_day.date()).days + 1
# remaining_days_in_month = (last_day - today.date()).days

# monthly_quota = daily_quota * (days_passed + remaining_days_in_month)
# attained_this_month = df_filtered[(df_filtered["application_date"] >= first_day) & (df_filtered["application_date"] <= today)].shape[0]
# percent_delivered = (attained_this_month / monthly_quota * 100) if monthly_quota else 0
# forecast_by_eom = int(attained_this_month / days_passed * (days_passed + remaining_days_in_month)) if days_passed else 0
# percent_forecast = (forecast_by_eom / monthly_quota * 100) if monthly_quota else 0

# # Format KPI table
# summary_table = pd.DataFrame({
#     "Monthly Quota": [f"{monthly_quota:,.0f}"],
#     "Delivered": [f"{attained_this_month:,.0f}"],
#     "%D": [f"{percent_delivered:.1f}%"],
#     "Forecast": [f"{forecast_by_eom:,.0f}"],
#     "%F": [f"{percent_forecast:.1f}%"]
# })

# # Display without index and center alignment
# st.markdown("""
# <style>
#     table {
#         margin-left: auto !important;
#         margin-right: auto !important;
#     }
#     thead tr th, tbody tr td {
#         text-align: center !important;
#         vertical-align: middle !important;
#     }
# </style>
# """, unsafe_allow_html=True)

# st.markdown("### 📅 Monthly KPI Summary")
# st.table(summary_table)

# # --- Chart Rendering ---
# view_levels = [("Monthly View", "M"), ("Weekly View", "W"), ("Daily View", "D")]
# for label, level in view_levels:
#     df_plot = prepare_grouped(df_filtered, level, this_year, last_year, today)
#     attained = df_filtered[df_filtered["application_date"].dt.year == this_year].shape[0]
#     remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
#     needed_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0
#     if level == "M":
#         needed_avg *= 30
#     elif level == "W":
#         needed_avg *= 7
#     st.markdown(f"## {label}")
#     plot_chart(df_plot, label, this_year, last_year, needed_avg, level)





# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
#                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # --- Monthly Forecast Table ---
# first_day = today.replace(day=1)
# last_day = (first_day + pd.offsets.MonthEnd(0)).date()
# days_passed = (today.date() - first_day.date()).days + 1
# remaining_days_in_month = (last_day - today.date()).days

# monthly_quota = daily_quota * (days_passed + remaining_days_in_month)
# attained_this_month = df_filtered[(df_filtered["application_date"] >= first_day) & (df_filtered["application_date"] <= today)].shape[0]
# percent_delivered = (attained_this_month / monthly_quota * 100) if monthly_quota else 0
# forecast_by_eom = int(attained_this_month / days_passed * (days_passed + remaining_days_in_month)) if days_passed else 0

# st.markdown("### 📅 Monthly KPI Summary")
# st.table(pd.DataFrame({
#     "Monthly Quota": [int(monthly_quota)],
#     "Delivered": [attained_this_month],
#     "%D": [f"{percent_delivered:.1f}%"],
#     "Forecast": [forecast_by_eom]
# }))

# # This year stats
# df_this_year = df_filtered[df_filtered["application_date"].dt.year == this_year]
# attained = len(df_this_year)
# remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
# needed_daily_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0

# # --- One Tab with All Views ---
# st.subheader(f"📅 {this_year} vs {last_year} — {nat}, {loc}, Active Visas: {'Yes' if active_only else 'No'}")

# views = [("M", "📆 Monthly View", (12 - today.month + 1)),
#          ("W", "📈 Weekly View", (52 - today.isocalendar().week + 1)),
#          ("D", "📅 Daily View", remaining_days)]

# for freq, label, remaining_periods in views:
#     df_plot = prepare_grouped(df_filtered, freq, this_year, last_year, today)
#     needed_avg = (total_quota - attained) / remaining_periods if remaining_periods > 0 else 0

#     fig = go.Figure()

#     if this_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[this_year],
#                                  mode='lines', name=str(this_year),
#                                  line=dict(color='green', width=2)))

#     if last_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[last_year],
#                                  mode='lines', name=str(last_year),
#                                  line=dict(color='gray', width=2, dash='dash')))

#     future_x = []
#     if freq == "M":
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         if today.month < 12:
#             next_month_abbr = month_abbrs[today.month]
#             future_x = [x for x in df_plot.index if str(x) == next_month_abbr or (x in month_abbrs and month_abbrs.index(str(x)) > today.month)]
#     elif freq == "W":
#         current_week = today.isocalendar().week
#         future_x = [x for x in df_plot.index if isinstance(x, (int, float)) and x > current_week]
#     elif freq == "D":
#         tomorrow = today + timedelta(days=1)
#         def safe_parse_date(x):
#             try:
#                 return datetime.strptime(f"{x}/{this_year}", "%d/%m/%Y")
#             except:
#                 return None
#         future_x = [x for x in df_plot.index if isinstance(x, str) and safe_parse_date(x) and safe_parse_date(x) > tomorrow]

#     if future_x:
#         future_y = [needed_avg] * len(future_x)
#         fig.add_trace(go.Scatter(x=future_x, y=future_y,
#                                  mode='lines', name='Needed Avg',
#                                  line=dict(color='red', dash='dash')))

#     fig.update_layout(title=label,
#                       xaxis_title='Date',
#                       yaxis_title='Applications',
#                       hovermode='x unified',
#                       xaxis_tickangle=0,
#                       yaxis_tickformat="~s")

#     if freq == "D":
#         fig.update_layout(xaxis=dict(
#             tickmode='array',
#             tickvals=[x for x in df_plot.index if x.startswith("01/") or x.startswith("15/")],
#             tickangle=0
#         ))

#     st.plotly_chart(fig, use_container_width=True)

# # --- KPI Box ---
# st.markdown(f"""
# ### 📌 Summary
# - 🎯 **Total Quota:** {int(total_quota):,}
# - ✅ **Applications Attained So Far ({this_year}):** {int(attained):,}
# - 🔴 **Required Daily Avg (Remaining Days):** {needed_daily_avg:.2f}
# """)










# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week.astype(int)
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
#                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # This year stats
# df_this_year = df_filtered[df_filtered["application_date"].dt.year == this_year]
# attained = len(df_this_year)
# remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
# needed_daily_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0

# # --- One Tab with All Views ---
# st.subheader(f"📅 {this_year} vs {last_year} — {nat}, {loc}, Active Visas: {'Yes' if active_only else 'No'}")

# views = [("M", "📆 Monthly View", (12 - today.month + 1)),
#          ("W", "📈 Weekly View", (52 - today.isocalendar().week + 1)),
#          ("D", "📅 Daily View", remaining_days)]

# for freq, label, remaining_periods in views:
#     df_plot = prepare_grouped(df_filtered, freq, this_year, last_year, today)
#     needed_avg = (total_quota - attained) / remaining_periods if remaining_periods > 0 else 0

#     fig = go.Figure()

#     if this_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[this_year],
#                                  mode='lines', name=str(this_year),
#                                  line=dict(color='green', width=2)))

#     if last_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[last_year],
#                                  mode='lines', name=str(last_year),
#                                  line=dict(color='gray', width=2, dash='dash')))

#     # Needed Avg Line: consistent logic across all views
#     future_x = []
#     if freq == "M":
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         if today.month < 12:
#             next_month_abbr = month_abbrs[today.month]
#             future_x = [x for x in df_plot.index if str(x) == next_month_abbr or (x in month_abbrs and month_abbrs.index(str(x)) > today.month)]
#     elif freq == "W":
#         current_week = today.isocalendar().week
#         future_x = [x for x in df_plot.index if isinstance(x, (int, float)) and x > current_week]
#     elif freq == "D":
#         tomorrow = today + timedelta(days=1)
#         def safe_parse_date(x):
#             try:
#                 return datetime.strptime(f"{x}/{this_year}", "%d/%m/%Y")
#             except:
#                 return None
#         future_x = [x for x in df_plot.index if isinstance(x, str) and safe_parse_date(x) and safe_parse_date(x) > tomorrow]

#     if future_x:
#         future_y = [needed_avg] * len(future_x)
#         fig.add_trace(go.Scatter(x=future_x, y=future_y,
#                                  mode='lines', name='Needed Avg',
#                                  line=dict(color='red', dash='dash')))

#     fig.update_layout(title=label,
#                       xaxis_title='Date',
#                       yaxis_title='Applications',
#                       hovermode='x unified',
#                       xaxis_tickangle=0,
#                       yaxis_tickformat="~s")

#     if freq == "D":
#         fig.update_layout(xaxis=dict(
#             tickmode='array',
#             tickvals=[x for x in df_plot.index if x.startswith("01/") or x.startswith("15/")],
#             tickangle=0
#         ))

#     st.plotly_chart(fig, use_container_width=True)

# # --- KPI Box ---
# st.markdown(f"""
# ### 📌 Summary
# - 🎯 **Total Quota:** {int(total_quota):,}
# - ✅ **Applications Attained So Far ({this_year}):** {int(attained):,}
# - 🔴 **Required Daily Avg (Remaining Days):** {needed_daily_avg:.2f}
# """)





# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
#                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')
#     elif level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')
#     elif level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # This year stats
# df_this_year = df_filtered[df_filtered["application_date"].dt.year == this_year]
# attained = len(df_this_year)
# remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
# needed_daily_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0

# # --- One Tab with All Views ---
# st.subheader(f"📅 {this_year} vs {last_year} — {nat}, {loc}, Active Visas: {'Yes' if active_only else 'No'}")

# views = [("M", "📆 Monthly View", (12 - today.month + 1)),
#          ("W", "📈 Weekly View", (52 - today.isocalendar().week + 1)),
#          ("D", "📅 Daily View", remaining_days)]

# for freq, label, remaining_periods in views:
#     df_plot = prepare_grouped(df_filtered, freq, this_year, last_year, today)
#     needed_avg = (total_quota - attained) / remaining_periods if remaining_periods > 0 else 0

#     fig = go.Figure()

#     if this_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[this_year],
#                                  mode='lines', name=str(this_year),
#                                  line=dict(color='green', width=2)))

#     if last_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[last_year],
#                                  mode='lines', name=str(last_year),
#                                  line=dict(color='gray', width=2, dash='dash')))

#     # Needed Avg Line: consistent logic across all views
#     future_x = []
#     if freq == "M":
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         if today.month < 12:
#             next_month_abbr = month_abbrs[today.month]
#             future_x = [x for x in df_plot.index if str(x) == next_month_abbr or (x in month_abbrs and month_abbrs.index(str(x)) > today.month)]
#     elif freq == "W":
#         current_week = today.isocalendar().week
#         future_x = [x for x in df_plot.index if isinstance(x, int) and x > current_week]
#     elif freq == "D":
#         tomorrow = today + timedelta(days=1)
#         def safe_parse_date(x):
#             try:
#                 return datetime.strptime(f"{x}/{this_year}", "%d/%m/%Y")
#             except:
#                 return None
#         future_x = [x for x in df_plot.index if isinstance(x, str) and safe_parse_date(x) and safe_parse_date(x) > tomorrow]

#     if future_x:
#         future_y = [needed_avg] * len(future_x)
#         fig.add_trace(go.Scatter(x=future_x, y=future_y,
#                                  mode='lines', name='Needed Avg',
#                                  line=dict(color='red', dash='dash')))

#     fig.update_layout(title=label,
#                       xaxis_title='Date',
#                       yaxis_title='Applications',
#                       hovermode='x unified',
#                       xaxis_tickangle=0,
#                       yaxis_tickformat="~s")

#     if freq == "D":
#         fig.update_layout(xaxis=dict(
#             tickmode='array',
#             tickvals=[x for x in df_plot.index if x.startswith("01/") or x.startswith("15/")],
#             tickangle=0
#         ))

#     st.plotly_chart(fig, use_container_width=True)

# # --- KPI Box ---
# st.markdown(f"""
# ### 📌 Summary
# - 🎯 **Total Quota:** {int(total_quota):,}
# - ✅ **Applications Attained So Far ({this_year}):** {int(attained):,}
# - 🔴 **Required Daily Avg (Remaining Days):** {needed_daily_avg:.2f}
# """)






# import streamlit as st
# from google.cloud import bigquery
# import pandas as pd
# from datetime import datetime, timedelta
# import plotly.graph_objects as go
# import os

# # Set Google Credentials (only if running locally)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-driven-attributes-957b43d1be08.json"

# # Initialize BigQuery client
# client = bigquery.Client()

# # --- Loaders ---
# @st.cache_data(ttl=3600)
# def load_applications():
#     query = """
#     SELECT
#         SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`Application Created`, 1, 10)) AS application_date,
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Activa Visa Status` AS active_visa_status
#     FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
#     WHERE `Application Created` IS NOT NULL
#     """
#     return client.query(query).to_dataframe()

# @st.cache_data(ttl=3600)
# def load_quotas():
#     query = """
#     SELECT
#         `Nationality Category Updated` AS nationality,
#         `Location Category Updated` AS location,
#         `Daily Quota Regardless Active Visas` AS quota_all,
#         `Daily Quota Considering Active Visas` AS quota_active
#     FROM `data-driven-attributes.AT_marketing_db.ATD_Daily_Quotas`
#     """
#     return client.query(query).to_dataframe()

# # --- Filters ---
# def filter_applications(df, nat, loc, active_only):
#     df = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if active_only and nat.lower() == "filipina" and loc.lower() == "philippines":
#         df = df[df["active_visa_status"] == "true"]
#     return df

# def get_daily_quota(df, nat, loc, active_only):
#     row = df[(df["nationality"] == nat) & (df["location"] == loc)]
#     if row.empty:
#         return 0
#     return row["quota_active"].iloc[0] if active_only else row["quota_all"].iloc[0]

# # --- Aggregation ---
# def prepare_grouped(df, level, this_year, last_year, today):
#     df = df.copy()
#     df["application_date"] = pd.to_datetime(df["application_date"])
#     df["year"] = df["application_date"].dt.year
#     df = df[df["year"].isin([this_year, last_year])]

#     if level == "M":
#         df["period"] = df["application_date"].dt.strftime('%b')
#         period_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         df["period"] = pd.Categorical(df["period"], categories=period_order, ordered=True)
#     elif level == "W":
#         df["period"] = df["application_date"].dt.isocalendar().week
#     elif level == "D":
#         df["period"] = df["application_date"].dt.strftime('%d/%m')

#     grouped = df.groupby(["period", "year"]).size().reset_index(name="applications")
#     pivoted = grouped.pivot(index="period", columns="year", values="applications")

#     if level == "M" and this_year in pivoted.columns:
#         latest_month = today.month
#         month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
#                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
#         later_months = month_abbrs[latest_month:]
#         pivoted.loc[pivoted.index.isin(later_months), this_year] = float('nan')

#     if level == "W" and this_year in pivoted.columns:
#         current_week = today.isocalendar().week
#         pivoted.loc[pivoted.index > current_week, this_year] = float('nan')

#     if level == "D" and this_year in pivoted.columns:
#         df_cutoff = df[(df["year"] == this_year) & (df["application_date"] <= today)]
#         valid_days = df_cutoff["period"].unique()
#         invalid_days = [day for day in pivoted.index if day not in valid_days]
#         pivoted.loc[invalid_days, this_year] = float('nan')

#     if level in ["W", "D"]:
#         pivoted = pivoted.sort_index(key=lambda x: [int(i.split('/')[1]) * 100 + int(i.split('/')[0]) if isinstance(i, str) else int(i) for i in x])

#     return pivoted

# # --- Streamlit UI ---
# st.title("📊 Application Volume Tracker")

# # Load data
# df_apps = load_applications()
# df_quotas = load_quotas()

# # Sidebar
# st.sidebar.header("🔍 Filters")
# nat = st.sidebar.selectbox("Nationality Category", sorted(df_apps["nationality"].dropna().unique()))
# loc = st.sidebar.selectbox("Location Category", sorted(df_apps["location"].dropna().unique()))
# active_only = st.sidebar.radio("Consider Active Visas?", ["Yes", "No"]) == "Yes"

# # Filtering
# df_filtered = filter_applications(df_apps, nat, loc, active_only)
# df_filtered["application_date"] = pd.to_datetime(df_filtered["application_date"])
# today = pd.to_datetime("today").normalize()
# this_year = today.year
# last_year = this_year - 1
# year_days = 366 if today.year % 4 == 0 else 365
# daily_quota = get_daily_quota(df_quotas, nat, loc, active_only)
# total_quota = daily_quota * year_days

# # This year stats
# df_this_year = df_filtered[df_filtered["application_date"].dt.year == this_year]
# attained = len(df_this_year)
# remaining_days = (pd.Timestamp(f"{this_year}-12-31") - today).days
# needed_daily_avg = (total_quota - attained) / remaining_days if remaining_days > 0 else 0

# # --- One Tab with All Views ---
# st.subheader(f"📅 {this_year} vs {last_year} — {nat}, {loc}, Active Visas: {'Yes' if active_only else 'No'}")

# views = [("M", "📆 Monthly View", (12 - today.month + 1)),
#          ("W", "📈 Weekly View", (52 - today.isocalendar().week + 1)),
#          ("D", "📅 Daily View", remaining_days)]

# for freq, label, remaining_periods in views:
#     df_plot = prepare_grouped(df_filtered, freq, this_year, last_year, today)
#     needed_avg = (total_quota - attained) / remaining_periods if remaining_periods > 0 else 0

#     fig = go.Figure()

#     if this_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[this_year],
#                                  mode='lines', name=str(this_year),
#                                  line=dict(color='green', width=2)))

#     if last_year in df_plot.columns:
#         fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[last_year],
#                                  mode='lines', name=str(last_year),
#                                  line=dict(color='gray', width=2, dash='dash')))

#     fig.add_trace(go.Scatter(x=df_plot.index, y=[needed_avg] * len(df_plot.index),
#                              mode='lines', name='Needed Avg',
#                              line=dict(color='red', dash='dash')))

#     fig.update_layout(title=label,
#                       xaxis_title='Date',
#                       yaxis_title='Applications',
#                       hovermode='x unified',
#                       xaxis_tickangle=0,
#                       yaxis_tickformat="~s")

#     st.plotly_chart(fig, use_container_width=True)

# # --- KPI Box ---
# st.markdown(f"""
# ### 📌 Summary
# - 🎯 **Total Quota:** {int(total_quota):,}
# - ✅ **Applications Attained So Far ({this_year}):** {int(attained):,}
# - 🔴 **Required Daily Avg (Remaining Days):** {needed_daily_avg:.2f}
# """)


