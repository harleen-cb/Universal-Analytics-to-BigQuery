import streamlit as st
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os
import datetime


# Streamlit UI
st.title('Universal Analytics Report Exporter to BigQuery')

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
file_path = st.text_input("Enter file path for the authentication key(json). Eg: '/home/user/Downloads/key.json'")
VIEW_ID = st.text_input("Enter UA view id")
BIGQUERY_PROJECT = st.text_input("Enter BQ project id")
BIGQUERY_DATASET = st.text_input("Enter BQ dataset")
BIGQUERY_TABLE = st.text_input("Enter BQ table name")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_path

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        file_path, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

# Request user to input dimensions and metrics


start_date = st.date_input("Start date")
end_date = st.date_input("End date")

selected_dimensions = st.multiselect(
    "Select dimensions",
    ['ga:date', 'ga:channelGrouping', 'ga:sourceMedium', 'ga:landingPagePath', 'ga:country', 'ga:campaign', 'ga:keyword', 'ga:adContent', 'ga:campaignCode', 'ga:adGroup', 'ga:adSlot', 'ga:browser', 'ga:browserVersion', 'ga:operatingSystem', 'ga:deviceCategory', 'ga:city', 'ga:pagePath', 'ga:pageTitle', 'ga:exitPagePath', 'ga:searchKeyword', 'ga:eventCategory', 'ga:eventAction', 'ga:eventLabel', 'ga:transactionId', 'ga:productSku', 'ga:productName', 'ga:channelGrouping', 'ga:goalCompletionLocation']
)

selected_metrics = st.multiselect(
    "Select metrics",
    ['ga:sessions', 'ga:users', 'ga:avgSessionDuration', 'ga:pageviewsPerSession', 'ga:goal1Completions', 'ga:goal2Completions', 'ga:goal3Completions', 'ga:goal4Completions', 'ga:goal5Completions', 'ga:goal6Completions', 'ga:newUsers', 'ga:bounceRate', 'ga:avgTimeOnPage', 'ga:totalEvents', 'ga:uniqueEvents', 'ga:eventValue', 'ga:transactions', 'ga:transactionsPerSession', 'ga:transactionRevenue', 'ga:revenuePerTransaction', 'ga:itemQuantity', 'ga:uniquePurchases', 'ga:itemRevenue', 'ga:itemsPerPurchase', 'ga:revenuePerItem' ]
)

# Display the selected option
st.write("You selected these dimensions:", selected_dimensions)

st.write("You selected these metrics:", selected_metrics)

dimensions = []
metrics= []

for selected_dimension in selected_dimensions:
                        dimensions.append({'name': selected_dimension})


for selected_metric in selected_metrics:
    metrics.append({'expression': selected_metric})



def get_report(analytics):
    """Fetches the report data from Google Analytics."""
    # Here, specify the analytics report request details
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'samplingLevel': 'LARGE',
                    'pageSize': 50000,
                    'dateRanges': [{'startDate': start_date.strftime("%Y-%m-%d"), 'endDate': end_date.strftime("%Y-%m-%d")}],
                    # Metrics and dimensions are specified here
                    'metrics': metrics,
                    'dimensions': dimensions,
                }
            ]
        }
    ).execute()

def response_to_dataframe(response):
    list_rows = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        for row in report.get('data', {}).get('rows', []):
            row_data = {header: dimension for header, dimension in zip(dimensionHeaders, row.get('dimensions', []))}
            for values in row.get('metrics', []):
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    row_data[metricHeader.get('name')] = value
            list_rows.append(row_data)
    return pd.DataFrame(list_rows)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Uploads the DataFrame to Google BigQuery."""
    # The DataFrame's column names are formatted for BigQuery compatibility
    df.columns = [col.replace('ga:', 'gs_') for col in df.columns]

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Generating schema based on DataFrame columns
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        else:
            bq_type = 'STRING'
        schema.append(bigquery.SchemaField(col, bq_type))

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        # Creating a new table if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Loading data into BigQuery and confirming completion
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded to {full_table_id}")



def main():
    """Main function to execute the script."""
    try:
        analytics = initialize_analyticsreporting()
        response = get_report(analytics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(f"Error occurred: {e}")



button_clicked = st.button("Export")
if button_clicked:
    main()  # Entry point of the script
