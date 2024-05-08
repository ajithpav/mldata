
from textwrap import dedent
import json
import pymongo
import time
from datetime import datetime
from datetime import timedelta
from datetime import date
from bson.json_util import dumps
from bson.objectid import ObjectId
import logging
import polaris_util as putil
import re
import sys
import mysql.connector
import ast
from airflow.models import Variable
import pytz
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator,ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import psutil
import numpy as np
# from pandas.io.json import json_normalize

DATA_SOURCE_NAME = "ServiceTitanBusinessAttribution"
DAG_NAME = "ServiceTitanBusinessAttributionTransform"
RAW_COLLECTION = "DataServiceTitanBusinessAttributionRaw"
TODAYS_DATE = date.today()
ENGAGEMENT_CALLS_RAW = "DataEngagementCallsTransformed"
TRANSFORM_CLLECTION_NAME = "DataServiceTitanBusinessAttributionTransformed"

try:
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE = Variable.get("SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE")
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT = Variable.get("SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT")
except KeyError:
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE = None
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT = None
    
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
putil.sentry_error_finder()
# function to get the calls from raw collection of service titan
def get_service_titan_business_attribution_raw_data(mongoUrl, dbName, raw_collectionName, tenantId, start_date, end_date, ingest_type) :
    client = pymongo.MongoClient(mongoUrl)
    polarisdb = client[dbName]
    raw_collection = polarisdb[raw_collectionName]
    filter_criteria = {
        "entity.tenantId": tenantId,
        "entity.date": {
            "$gte": start_date,
            "$lt": end_date,
        }
    }
    print("filter_criteria",filter_criteria)

    # if ingest_type == "net_change":
    #     date_from, date_to = putil.get_changed_records_ingestion_dates()
    #     filter_criteria["lastUpdated"] = {
    #         "$gte": date_from,
    #         "$lt": date_to
    #     }

    documents_cursor = raw_collection.find(filter_criteria,{"_id": 0})
    formated_response=list(documents_cursor)
    print(len(formated_response))
    if formated_response is not None:
        return formated_response
    else:
        print("NO DATA FOUND")



# fucntion to get the  calls data from the engagement raw collection
def get_engagementCalls_raw_data(mongoUrl, dbName, raw_collectionName, tenantId, start_date, end_date, ingest_type):
    client = pymongo.MongoClient(mongoUrl)
    polarisdb = client[dbName]
    raw_collection = polarisdb["DataEngagementCallsTransformed"]
    filter_criteria = {
        "entity.tenantId": tenantId,
        "entity.date": {
            "$exists": True,
            "$gte": start_date,
            "$lt": end_date,
        },
        "dateClientTz": {"$exists": True},
        "dateLocationTz": {"$exists": True},
        "dateUTCTz": {"$exists": True}
    }

    # Aggregation pipeline to exclude "clientRegion" field from the entity
    pipeline = [
        {"$match": filter_criteria},
        {"$addFields": {
            "entity_filtered": {
                "$cond": {
                    "if": {"$isArray": "$entity"},
                    "then": {
                        "$map": {
                            "input": "$entity",
                            "in": {
                                "$cond": {
                                    "if": {"$eq": ["$$this.clientRegion", [None]]},
                                    "then": "$$REMOVE",
                                    "else": "$$this"
                                }
                            }
                        }
                    },
                    "else": "$entity"
                }
            }
        }},
        {"$unwind": "$entity_filtered"},
        {"$project": {
            "_id": 0,
            "date": "$entity_filtered.date",
            "lastUpdated": "$lastUpdated",
            "entity": {
                "nDate": "$entity_filtered.nDate",
                "tenantId": "$entity_filtered.tenantId",
                "source": "$entity_filtered.source",
                "medium": "$entity_filtered.medium",
                "campaign": "$entity_filtered.campaign",
                "campaignGroup": "$entity_filtered.campaignGroup",
                "campaignGroupId": "$entity_filtered.campaignGroupId",
                "campaignId": "$entity_filtered.campaignId",
                "channelNameId": "$entity_filtered.channelNameId",
                "channelId": "$entity_filtered.channelId",
                "platformId": "$entity_filtered.platformId",
                "campaignTypeId": "$entity_filtered.campaignTypeId",
                "channelTypeName": "$entity_filtered.channelTypeName",
                "channelName": "$entity_filtered.channelName",
                "platformName": "$entity_filtered.platformName",
                "campaignTypeName": "$entity_filtered.campaignTypeName",
                "locationId": "$entity_filtered.locationId",
                "locationGroupId": "$entity_filtered.locationGroupId",
                "marketSegmentId": "$entity_filtered.marketSegmentId",
                "noaaRegion": "$entity_filtered.noaaRegion",
                "nielsonRegion": "$entity_filtered.nielsonRegion",
                "content": "$entity_filtered.content",
                "key": "$entity_filtered.key",
                "dayOfWeek": "$entity_filtered.dayOfWeek",
                "hourOfDay": "$entity_filtered.hourOfDay",
                "hour": "$entity_filtered.hour"
            },
            "dimensions": "$dimensions",
            "metrics": "$metrics",
            "dateClientTz": "$dateClientTz",
            "dateLocationTz": "$dateLocationTz",
            "dateUTCTz": "$dateUTCTz"
        }}
    ]
    documents_cursor = raw_collection.aggregate(pipeline)
    formated_response=list(documents_cursor)
    # print("formated_response length",len(formated_response))
    if formated_response is not None:
        return formated_response
    else:
        print("NO DATA FOUND")


# function to match the ST and Engagment calls data
def match_st_data_with_eng_data(**kwargs):
        retrieved_params = kwargs['ti'].xcom_pull(key='get_data_for_inputs_task_args', task_ids='get_data_for_inputs_task')
        retrieved_params_for_log_from_extract = kwargs['ti'].xcom_pull(key='start_transform_task_inputs', task_ids='start_transform_task')
        raw_data_st_df_raw=retrieved_params['raw_data_st_df']
        # raw_data_st_df_raw= pd.DataFrame(raw_data_st_df_raw)
        raw_data_eng_calls_df=retrieved_params['raw_data_eng_calls_df']
        # raw_data_eng_calls_df = pd.DataFrame(raw_data_eng_calls_df)
        retrieved_params_for_log=retrieved_params_for_log_from_extract['config_logs']
        sysDataSourceConfig=retrieved_params_for_log_from_extract["sysDataSourceConfig"]
        ingest_type = retrieved_params_for_log['ingest_type']
        # execution_date = kwargs.get('execution_date', None)
        # if execution_date:
        #     task_instance = TaskInstance(task_id='get_data_for_inputs_task', execution_date=execution_date)
        #     task_instance.xcom_delete(key='extract_task_params')
        category=None
        if "category" in sysDataSourceConfig[0]:
            category = sysDataSourceConfig[0]['category']  
        else:
            category = ""

        if "ingestionSchedule" in sysDataSourceConfig[0]:
            if ("dailyLoad" in sysDataSourceConfig[0]["ingestionSchedule"]) and (ingest_type == "daily"):
                ingestionSchedule = sysDataSourceConfig[0]["ingestionSchedule"]['dailyLoad']
            elif ("netChange" in sysDataSourceConfig[0]["ingestionSchedule"]) and (ingest_type == "net_change"):
                ingestionSchedule = sysDataSourceConfig[0]["ingestionSchedule"]['netChange']
            else:
                ingestionSchedule = ""
        else:
            ingestionSchedule = ""

        # Extract the 'dimensions' column into a DataFrame
        raw_data_st_dimensions_df = pd.DataFrame(raw_data_st_df_raw['dimensions'].tolist())
        raw_data_cl_dimensions_df = pd.DataFrame(raw_data_eng_calls_df['dimensions'].tolist())
        raw_data_cll_metrics_df = pd.DataFrame(raw_data_eng_calls_df['metrics'].tolist())
        # raw_data_st_dimensions_df['tenantId']=raw_data_cll_metrics_df['entity']['tea']
        ingest_type=retrieved_params_for_log['ingest_type']
        ingest_start_date=retrieved_params_for_log['ingest_start_date']
        ingest_end_date=retrieved_params_for_log['ingest_end_date']
        tenant_id=retrieved_params_for_log['tenantId']


        #getting the connection string    
        db = putil.get_mongoDb_details()



        # print("count of raw_data_st_dimensions_df",raw_data_st_dimensions_df.count(axis=0))
        result_cll_df = pd.concat([raw_data_eng_calls_df, raw_data_cl_dimensions_df, raw_data_cll_metrics_df], axis=1)
        result_st_df = pd.concat([raw_data_st_df_raw, raw_data_st_dimensions_df], axis=1)
        # for matcching time
        result_cll_df['call_start'] = result_cll_df['call_start'].apply(lambda x: datetime.strptime(x['$date'], '%Y-%m-%dT%H:%M:%SZ'))
        result_st_df['receivedOn'] = result_st_df['receivedOn'].apply(lambda x: datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S'))
    

        
        #print("records of st before merge:::",result_st_df.iloc[:1].to_dict())
        print("Length of result_st_df",len(result_st_df))
        
        #print("records of Calls before merge:::",result_cll_df.iloc[:1].to_dict())
        print("Length of result_cll_df",len(result_cll_df))


        # Print the first 5 rows of result_st_df with the new fields
        print("result_st_df with new fields:")
       # print(result_st_df[['receivedDate', 'receivedDate1', 'StMinutesFromMidnight']].head())
        print("Size in MB of result_st_df dataframe:",result_st_df.memory_usage(index=True).sum() / (1024 * 1024))

        # Add 'Date' and 'MinutesFromMidnight' fields to result_cll_df
        # result_cll_df['callDate'] = pd.to_datetime(result_cll_df['call_start']).dt.date
        # result_cll_df['callDate1'] = pd.to_datetime(result_cll_df['call_start']).dt.date
        result_cll_df['CallsMinutesFromMidnight'] = result_cll_df['call_start'].dt.hour * 60 + result_cll_df['call_start'].dt.minute

        # Convert 'MinutesFromMidnight' to integer
        result_cll_df['CallsMinutesFromMidnight'] = result_cll_df['CallsMinutesFromMidnight'].astype(int)

        # Print the first 5 rows of result_cll_df with the new fields
        print("result_cll_df with new fields:")
        #print(result_cll_df[['call_start', 'callDate1', 'CallsMinutesFromMidnight']].head())
        print("Size in MB of result_cll_df dataframe:",result_cll_df.memory_usage(index=True).sum() / (1024 * 1024))
        

        #print("records of After merge:::", merged_df.iloc[:11].to_dict())
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")




        def service_titen(st_record, engagement_call_data, store_date, invoice_duplicate) :
            engagement_call_data = engagement_call_data[engagement_call_data['caller_number'] == st_record["fromPhoneNumber"]]
            engagement_call_data = engagement_call_data[engagement_call_data['forwardno'] == st_record["toPhoneNumber"]]
            st_record["receivedOn"] = pd.to_datetime(st_record["receivedOn"])
            engagement_call_data['call_start'] = pd.to_datetime(engagement_call_data['call_start'])
            network_minutes = pd.to_timedelta(engagement_call_data['network_minutes'], unit='m')
            engagement_call_data['start_received'] = engagement_call_data['call_start'] - st_record["receivedOn"]
            callDuration = pd.to_timedelta(st_record['callDuration'])
            engagement_call_data['call_network_min'] = callDuration - network_minutes
            if st_record['invoiceId'] in invoice_duplicate:
                print("Duplicate invoice found!")
                st_record['metrics']['revenue'] = 0.00
            else :
                invoice_duplicate.append(st_record['invoiceId'])

            if len(engagement_call_data) == 1:
                engagement_call_start_filter = engagement_call_data[abs(engagement_call_data['start_received']) < pd.Timedelta(minutes=1)]

                if len(engagement_call_start_filter):
                    engagement_call_network_filter = engagement_call_start_filter[abs(engagement_call_start_filter['call_network_min']) < pd.Timedelta(minutes=1)]

                    if len(engagement_call_network_filter) and not len(engagement_call_network_filter[engagement_call_network_filter['call_id'].isin(store_date)]):
                        store_date.append(engagement_call_network_filter['call_id'].to_string(index=False))
                        return add_eng_dimensions(engagement_call_network_filter.reset_index(drop=True))

            elif len(engagement_call_data) > 1 :
                engagement_call_data['start_received'] = engagement_call_data['start_received'].abs()
                engagement_call_start_filter = engagement_call_data[engagement_call_data['start_received'] < pd.Timedelta(minutes=1)]
                engagement_call_network_filter = engagement_call_start_filter[abs(engagement_call_start_filter['call_network_min']) < pd.Timedelta(minutes=1)]

                if len(engagement_call_network_filter) > 1:
                    engagement_call_data_min = engagement_call_network_filter[engagement_call_network_filter['start_received'] == engagement_call_network_filter['start_received'].min()]
                    if len(engagement_call_data_min) and not len(engagement_call_data_min[engagement_call_data_min['call_id'].isin(store_date)]):
                        store_date.append(engagement_call_data_min['call_id'].to_string(index=False))
                        return add_eng_dimensions(engagement_call_data_min.reset_index(drop=True))
                else:
                    if len(engagement_call_network_filter) and not len(engagement_call_network_filter[engagement_call_network_filter['call_id'].isin(store_date)]):
                        store_date.append(engagement_call_network_filter['call_id'].to_string(index=False))
                        return add_eng_dimensions(engagement_call_network_filter.reset_index(drop=True))

            if len(engagement_call_data) :
                engagement_call_data = engagement_call_data[abs(engagement_call_data['start_received']) < pd.Timedelta(minutes=61)]
                engagement_call_data = engagement_call_data[engagement_call_data['start_received'] == engagement_call_data['start_received'].min()]
                engagement_call_data = engagement_call_data[abs(engagement_call_data['call_network_min']) < pd.Timedelta(minutes=1)].reset_index(drop=True)
                engagement_call_data = engagement_call_data.head(1)
                print("61 min matching  :", len(engagement_call_data))
                if len(engagement_call_data) and not len(engagement_call_data[engagement_call_data['call_id'].isin(store_date)]):
                    store_date.append(engagement_call_data['call_id'].to_string(index=False))
                    return add_eng_dimensions(engagement_call_data.reset_index(drop=True))

            return add_eng_dimensions([])

        def add_eng_dimensions(engagement_call_data):
            # print("engagement_call_data", engagement_call_data)
            import numpy as np
            return_dict = {}
            return_dict['dimensions_rrp_calls'] = None
            return_dict['dateClientTz'] = None
            return_dict['dateUTCTz'] = None
            return_dict['dateLocationTz'] = None
            return_dict['call_start'] = None
            return_dict['entity'] = {}
            return_dict['network_minutes'] = None
            # return_dict['entity'] = i.get("entity")
            return_dict['is_engagement_match_found']= 0
            # print("engagement_call_data", len(engagement_call_data), engagement_call_data.columns)
            print("matched data count", len(engagement_call_data))
            if len(engagement_call_data):
                col = engagement_call_data.columns

                return_dict['dimensions_rrp_calls'] = engagement_call_data["dimensions"][0]
                return_dict['dateClientTz'] = engagement_call_data["dateClientTz"][0]
                return_dict['dateUTCTz'] = engagement_call_data["dateUTCTz"][0]
                return_dict['dateLocationTz'] = engagement_call_data["dateLocationTz"][0]
                return_dict['call_start'] = engagement_call_data["call_start"][0]
                return_dict['network_minutes'] = engagement_call_data["network_minutes"][0]
                return_dict['entity'] = engagement_call_data["entity"][0] if 'entity' in col else {}
                # return_dict['entity'] = i.get("entity")
                return_dict['is_engagement_match_found'] = 1

            return return_dict

        store_date = []
        invoice_duplicate = []
        result_st_df.rename(columns={'entity': 'entity_st'}, inplace=True)
        result_st_df['object_dim'] = result_st_df.apply(service_titen, args=[result_cll_df, store_date, invoice_duplicate], axis= 1)
        dimention_df = pd.DataFrame(result_st_df['object_dim'].to_list())
        result_st_df.drop(columns=['object_dim'], inplace=True)
        # result_st_df = result_st_df[['date', 'entity', 'dimensions', 'network_minutes', 'call_start', 'dimensions_rrp_calls', 'dateClientTz', 'dateUTCTz', 'dateLocationTz',]]
        del store_date, invoice_duplicate

        result_st_df = pd.concat([result_st_df,dimention_df], axis=1)
        from pandas import Timestamp
        import numpy as np
        def update_entity(row):
                if not pd.isna(row['entity']) and len(row['entity']) > 3 :
                    entity_data = {
                        'tenantId': row['entity']['tenantId'],
                        'date': Timestamp(row['date']['$date']).to_pydatetime(),
                        "nDate": row['entity']['nDate'],
                        "source": row['entity']['source'],
                        "medium": row['entity']['medium'],
                        "campaign": row['entity']['campaign'],
                        "campaignGroup": row['entity']['campaignGroup'],
                        "campaignGroupId": row['entity']['campaignGroupId'],
                        "campaignId": row['entity']['campaignId'],
                        "channelNameId": row['entity']['channelNameId'],
                        "channelId": row['entity']['channelId'],
                        "platformId": row['entity']['platformId'],
                        "clientRegion":'',
                        "campaignTypeId": row['entity']['campaignTypeId'],
                        "channelTypeName": row['entity']['channelTypeName'],
                        "channelName": row['entity']['channelName'],
                        "platformName": row['entity']['platformName'],
                        "campaignTypeName": row['entity']['campaignTypeName'],
                        "locationId": row['entity']['locationId'],
                        "locationGroupId": row['entity']['locationGroupId'],
                        "marketSegmentId": row['entity']['marketSegmentId'],
                        "nielsonRegion": row['entity']["nielsonRegion"],
                        "dayOfWeek": row['entity']['dayOfWeek'],
                        "hourOfDay": row['entity']['hourOfDay'],
                        "hour": row['entity']["hour"]
                        }
                    return entity_data
                else:
                    row['entity_st']['date'] = pd.to_datetime(row['entity_st']['date']["$date"]) if row['entity_st'] and "$date" in row['entity_st']['date']   else row['entity_st']['date']
                    return row['entity_st']


        result_st_df['entity'] = result_st_df.apply(update_entity, axis=1)
        result_st_df.drop(columns=['entity_st'], inplace=True)

        matches_filtered = result_st_df
        # Apply create_metrics function to each row
        def create_metrics(row, jobNumberlst):
            # is_jobNumber_unique = 0
            metrics = {
                "is_engagement_match_found": row['is_engagement_match_found'],
                # "is_jobNumber_unique": 0,
                "is_jobBooked": 0,
                "is_jobScheduled": 0,
                "is_jobInProgress": 0,
                "is_jobHold": 0,
                "is_jobCanceled": 0,
                "is_job_matched": 0
            }
              # Initialize an empty list to store unique job numbers
            # Check if jobNumber is unique and update metrics accordingly
            # if row['dimensions']['jobNumber'] not in jobNumberlst:
            #     jobNumberlst.append(row['dimensions']['jobNumber'])
            #     is_jobNumber_unique = 1
                # metrics['is_jobNumber_unique'] = 1
                
                    
            # Update metrics based on various conditions
            if  metrics['is_engagement_match_found'] == 1 :
                job_status = row['dimensions']['jobStatus']
                print("job_status", job_status)
                metrics['revenue'] = row['metrics']['revenue']
                if job_status == "Completed":
                    metrics['is_jobBooked'] = 1
                elif job_status == "Scheduled":
                    metrics['is_jobScheduled'] = 1
                elif job_status == "InProgress":
                    metrics['is_jobInProgress'] = 1
                elif job_status == "Hold":
                    metrics['is_jobHold'] = 1
                elif job_status == "Canceled":
                    metrics['is_jobCanceled'] = 1
                        
                metrics['is_job_matched'] = 1 if job_status else 0
                    
            return metrics

        jobNumberlst = []
        matches_filtered['metrics'] = matches_filtered.apply(create_metrics, args=[jobNumberlst] ,  axis=1)
        print("jobNumberlst", jobNumberlst)
        del jobNumberlst
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")
        # Convert to list of records
        
        matches_records = matches_filtered.to_dict(orient='records')
        result_df =matches_filtered # pd.DataFrame(matches_records)
        # Copy 'st' entity fields

        def create_date(date_object):
            format_str = '%Y-%m-%dT%H:%M:%SZ'  # Updated format string without microseconds
            if isinstance(date_object, dict) and '$date' in date_object:
                try:
                    return datetime.strptime(date_object['$date'], format_str)
                except ValueError as e:
                    print("Time data does not include microseconds:", e)
            return np.nan
        def create_date_key(entity_dict) :
            if isinstance(entity_dict, dict):
                updated_entity = entity_dict.copy()  # Create a copy of the original entity dictionary
                date_value = updated_entity.get('$date')
                dt_object = datetime.strptime(date_value, "%Y-%m-%dT%H:%M:%SZ")
                dt_object = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
                if dt_object:
                    updated_entity['lastUpdated'] = datetime.strptime(dt_object['$date'], '%Y-%m-%dT%H:%M:%SZ')
                return updated_entity
        result_df['dateClientTz'] =result_df['dateClientTz'].apply(create_date)
        result_df['dateUTCTz'] =result_df['dateUTCTz'].apply(create_date)
        result_df['dateLocationTz'] =result_df['dateLocationTz'].apply(create_date)
        result_df['lastUpdated'] = datetime.strptime(datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"), "%Y-%m-%dT%H:%M:%S.%f+00:00")
        # result_df['lastUpdated'] = pd.to_datetime(result_df['dateLocationTz']).apply(create_date)
        result_df['dateClientTz'] = result_df['dateClientTz'].replace({pd.NaT: None})
        result_df['dateUTCTz'] = result_df['dateUTCTz'].replace({pd.NaT:  None})
        result_df['dateLocationTz'] = result_df['dateLocationTz'].replace({pd.NaT: None})
        result_df['lastUpdated'] = result_df['lastUpdated'].replace({pd.NaT: None})

        result_df = result_df.replace({pd.NaT: None})
        print("matches_filtered",len(matches_filtered), 'result_df', len(result_df))

        print(len(result_df),"result_df.columns",result_df.columns)
        result_df['date'] = matches_filtered['receivedOn']
        result_df['dimensions'] = matches_filtered['dimensions']
        #Droping unwanted fileds
        result_df = result_df[['date','jobNumber', 'callId', 'entity','dimensions_rrp_calls' , 'dimensions', 'metrics', 'dateClientTz', 'dateUTCTz', 'dateLocationTz' ]]


        # Process 'entity' fields
        print("matches Filtered Length before ",len(matches_filtered))
        print("matches Filtered size in MB before ",matches_filtered.memory_usage(index=True).sum() / (1024 * 1024))
        # result_df['lastUpdated'] = matches_filtered['lastUpdated'].apply(create_date)
        print("result_df Filtered Length after ",len(result_df))

        print("resultdf after new metrics length",len(result_df))
        # result_df.drop(['entity_cll', 'entity_st'], axis=1, inplace=True)





        # print("result_df",result_df.iloc[0]['entity'])
        # unique_callid_df = result_df.drop_duplicates(subset='invoiceId', keep='first')
        unique_callid_df = result_df

        #Drop Duplicates by invoice id
        result_df_for_jobs = result_df
        raw_matched_calls_df = result_df



        # matched raw data of calls and jobs data before ingesting into transform collection
        formated_matched_calls_data_for_raw_collection  = raw_matched_calls_df.to_dict(orient='records')
        formated_matched_jobs_data_for_raw_collection = result_df_for_jobs.to_dict(orient='records')



        # this is being added to pull the unique jobs data to into a diffrent collection
        # result_df_for_jobs = result_df_for_jobs.dropna(subset=['dateLocationTz', 'dateUTCTz', 'dateClientTz'])
        job_de_dup_df = result_df_for_jobs.drop_duplicates(subset=['jobNumber'], keep='first')
        job_de_dup_df = job_de_dup_df.dropna(subset=['dateLocationTz', 'dateUTCTz', 'dateClientTz'])
        job_de_dup_df = job_de_dup_df.dropna(subset=['jobNumber'])
        formated_records_job_de_dup_for_transformed = job_de_dup_df.to_dict(orient='records')



        # matched calls data calculate the calls matched metric
        # raw_matched_calls_df = result_df_for_jobs.drop_duplicates(subset=['call_id'], keep='first')
        raw_matched_calls_df = result_df_for_jobs.drop_duplicates(subset=['callId'], keep='first')
        raw_matched_calls_df = raw_matched_calls_df.dropna(subset = ['dateLocationTz', 'dateUTCTz', 'dateClientTz'])
        formated_matched_calls_data_for_transformed = raw_matched_calls_df.to_dict(orient='records')
        print("length of extract_st_jobs_raw_collection",len(formated_records_job_de_dup_for_transformed))
        print("Length of matched_calls_raw_collection",len(formated_matched_calls_data_for_transformed))




        # extract raw and transform collections for individual metrics of calls and jobs
        extract_st_jobs_raw_collection = db["DataServiceTitanBusinessAttributionJobsDataRaw"]
        matched_calls_raw_collection = db["DataServiceTitanBusinessAttributionCallsDataRaw"]
        extract_st_jobs_transformed_collection = db["DataServiceTitanBusinessAttributionJobsDataTransformed"]
        matched_calls_transformed_collection = db["DataServiceTitanBusinessAttributionCallsDataTransformed"]



        start_date_obj = datetime.strptime(ingest_start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(ingest_end_date, "%Y-%m-%d")
        # Format the strings to remove microseconds
        ISOStartDate = start_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        ISOEndDate = end_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

        
        # cleaning up the the raw collection 
        match_query = {
            "$match": {
                "entity.tenantId": tenant_id,
                "$expr": {
                    "$and": [
                        {
                            "$gte": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOStartDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        },
                        {
                            "$lt": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOEndDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        }



        ### Keeping the raw data before transforming the data
        try:
            # Delete existing records based on the match query for extract_st_jobs_raw_collection
            delete_result_for_raw_jobs = extract_st_jobs_raw_collection.delete_many(match_query['$match'])
            # Print the number of records deleted
            num_records_deleted_jobs = delete_result_for_raw_jobs.deleted_count
            print(f"{num_records_deleted_jobs} record(s) deleted for extract_st_jobs_raw_collection.")
            result_of_inserting_jobs_data = extract_st_jobs_raw_collection.insert_many(formated_matched_calls_data_for_raw_collection) 
            print("Inserted doc's in to the jobs raw collection",result_of_inserting_jobs_data)

            # Delete existing records based on the match query for matched_calls_raw_collection
            delete_result_for_raw_calls = matched_calls_raw_collection.delete_many(match_query['$match'])
            # Print the number of records deleted
            num_records_deleted_calls = delete_result_for_raw_calls.deleted_count
            print(f"{num_records_deleted_calls} record(s) deleted for matched_calls_raw_collection.")
            result_of_inserting_calls_data = matched_calls_raw_collection.insert_many(formated_matched_jobs_data_for_raw_collection)           
            print("Inserted doc's in to the calls raw collection",result_of_inserting_calls_data)

        except Exception as e:
            print(f"An error occurred while inserting raw data for calls and jobs: {e}")




        ### transforming the data and updating into the collection
        try:
            # Delete existing records based on the match query for extract_st_jobs_transformed_collection
            delete_result_form_transformed_collection_jobs = extract_st_jobs_transformed_collection.delete_many(match_query['$match'])
            # Print the number of records deleted
            num_records_deleted_jobs_from_transform = delete_result_form_transformed_collection_jobs.deleted_count
            print(f"{num_records_deleted_jobs_from_transform} record(s) deleted for extract_st_jobs_transformed_collection.")


            result_of_inserting_jobs_data_into_tansform = extract_st_jobs_transformed_collection.insert_many(formated_records_job_de_dup_for_transformed) 
            print("Inserted doc's in to the jobs transform collection",result_of_inserting_jobs_data_into_tansform)

            # Delete existing records based on the match query for matched_calls_transformed_collection
            delete_result_for_transform_calls = matched_calls_transformed_collection.delete_many(match_query['$match'])
            # Print the number of records deleted
            num_records_deleted_calls_transform_collection = delete_result_for_transform_calls.deleted_count
            print(f"{num_records_deleted_calls_transform_collection} record(s) deleted for matched_calls_transformed_collection.")
            result_of_inserting_calls_data_transform_collection = matched_calls_transformed_collection.insert_many(formated_matched_calls_data_for_transformed)           
            print("Inserted doc's in to the calls raw collection",result_of_inserting_calls_data_transform_collection)

        except Exception as e:
            print(f"An error occurred while inserting raw data for calls and jobs: {e}")

          
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")
        jobs_data_aggregated_collection = db["DataServiceTitanBusinessAttributionJobsDataAggregated"]
        calls_data_aggregated_collection = db["DataServiceTitanBusinessAttributionCallsDataAggregated"]


        #dataJobsAggregationConf
        jobs_aggregation_config_str = sysDataSourceConfig[0]['configurations'].get("dataJobsAggregationConf")
        jobs_aggregation_config_str = "["+jobs_aggregation_config_str+"]"
        jobs_aggregation_config_dict= json.loads(re.sub(r'\s+', '', jobs_aggregation_config_str))
        # print("aggregation_config_dict",type(jobs_aggregation_config_dict))
        print("aggregation_config_dict length",len(jobs_aggregation_config_dict) )



        #dataCallsAggregationConf
        calls_aggregation_config_str = sysDataSourceConfig[0]['configurations'].get("dataCallsAggregationConf")
        calls_aggregation_config_str = "["+calls_aggregation_config_str+"]"
        calls_aggregation_config_dict= json.loads(re.sub(r'\s+', '', calls_aggregation_config_str))
        #print("calls_aggregation_config_dict",type(calls_aggregation_config_dict))
        print("calls_aggregation_config_dict",len(calls_aggregation_config_dict))




        # jobs data metric processing using aggregation pipelines
        match_query = {
            "$match": {
                "entity.tenantId": tenant_id,
                "$expr": {
                    "$and": [
                        {
                            "$gte": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOStartDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        },
                        {
                            "$lt": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOEndDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        }
        pipeline=[]
        if jobs_aggregation_config_dict is not None:
            pipeline = [
                        match_query,
                        jobs_aggregation_config_dict[0],
                        jobs_aggregation_config_dict[1],
                    ]
            print("aggregation query", pipeline)
            # Performing the aggregation
            result_cursor = extract_st_jobs_transformed_collection.aggregate(pipeline)
            result_list = list(result_cursor)
            converted_arr_dic = [dict(document) for document in result_list]
            try:
                # Delete existing records based on the match query
                delete_result=jobs_data_aggregated_collection.delete_many(match_query['$match'])
                # Print the number of records deleted
                num_records_deleted = delete_result.deleted_count
                print(f"{num_records_deleted} record(s) deleted.")
                result_of_inserting = jobs_data_aggregated_collection.insert_many(converted_arr_dic)    
            except Exception as e:
                print("err inserting for jobs data", str(e))
        


        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")
        # calls data metrics processing
        if calls_aggregation_config_dict is not None:
            pipeline = [
                        match_query,
                        calls_aggregation_config_dict[0],
                        calls_aggregation_config_dict[1],
                    ]
            print("Pipeline CallsData Aggregate:" , pipeline)
            # Performing the aggregation
            result_cursor = matched_calls_transformed_collection.aggregate(pipeline)
            result_list = list(result_cursor)
            converted_Calls_arr_dic = [dict(document) for document in result_list]
            try:
                # Delete existing records based on the match query
                delete_result=calls_data_aggregated_collection.delete_many(match_query['$match'])
                # Printing the number of records deleted
                num_records_deleted = delete_result.deleted_count
                print(f"{num_records_deleted} record(s) deleted Calls Aggregated.")
                result_of_inserting = calls_data_aggregated_collection.insert_many(converted_Calls_arr_dic)    
            except Exception as e:
                print("err inserting for Calls data Aggregate", str(e))

        #unique_callid_df = result_df.drop_duplicates(subset=['call_id'], keep='first')
        # print("unique_callid_df",unique_callid_df)



        # Convert to list of records
        formated_records = unique_callid_df.to_dict(orient='records')

        custom_file_name = f'sttransform_{tenant_id}_{datetime.now()}.csv'

        formated_records = unique_callid_df.to_dict(orient='records')
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")
        # Print the first record to check the format
        # result_records = merged_df.to_dict('records')
        # print("result_records len", len(result_records))
        print("formated_records",formated_records, '--------')
        if formated_records:
            putil.insert_servictian_calls_src_data_into_mongo_transform_collection(formated_records,TRANSFORM_CLLECTION_NAME,ingest_type,ingest_start_date,ingest_end_date,tenant_id,match_query)
            putil.insert_ingestion_summary_log_with_config(retrieved_params_for_log_from_extract['config_logs'],TRANSFORM_CLLECTION_NAME,category, ingestionSchedule )






def get_data_for_inputs(**kwargs):
    retrieved_params = kwargs['ti'].xcom_pull(key='start_transform_task_inputs', task_ids='start_transform_task')
    ingest_start_date=retrieved_params['ingest_start_date']
    ingest_end_date=retrieved_params['ingest_end_date']
    tenant_id=retrieved_params['tenant_id']
    ingest_type=retrieved_params['ingest_type']
    
    print("ingest_start_date, ingest_end_date, tenant_id,ingest_type",ingest_start_date, ingest_end_date, tenant_id,ingest_type)
    sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
    tenantDetails = json.loads(dumps(putil.get_tenant_datasource_details(sysDataSourceConfig[0]["platform"],tenant_id, DAG_NAME)))     
    # tenant_locations = putil.get_tenant_locations(tenant_id)
    # tenant_locationGroups = putil.get_tenant_location_groups(tenant_id)
    # tenantmarketSegments = putil.get_tenant_market_segments(tenant_id)
    # RAW_COLLECTION = sysDataSourceConfig[0]['destinationCollections']["collectionRaw"]
    # TRANSFORMED_COLLECTION = sysDataSourceConfig[0]['destinationCollections']["collectionTransformed"]
    RAW_COLLECTION_FOR_ST="DataServiceTitanBusinessAttributionRaw"
    # TRANSFORMED_COLLECTION = "TransformSampleSTCallsData"
    if tenantDetails is not None and len(tenantDetails) != 0:
        tenantDetail = tenantDetails[0]
        customer_ids = tenantDetail['accessInfo']                
        # start_iso = datetime.strptime(ingest_start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        # end_iso = datetime.strptime(ingest_end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_iso = datetime.strptime(ingest_start_date, "%Y-%m-%d")
        end_iso = datetime.strptime(ingest_end_date, "%Y-%m-%d")
        customer_id = customer_ids[0]
        client_id, client_secret, client_tenant_id = customer_id["clientId"], customer_id["clientSecret"], customer_id["tenantId"]

        print("customer_ids,start_iso,end_iso,customer_id,client_id,client_secret,client_tenant_id",customer_ids,start_iso,end_iso,customer_id,client_id,client_secret,client_tenant_id)

        # Fetch raw data
        rawcollectiondataServiceTitan = json.loads(dumps(get_service_titan_business_attribution_raw_data(
            putil.MONGO_URL,
            putil.MONGO_DBNAME,
            RAW_COLLECTION_FOR_ST,
            tenantDetail['tenantId'],
            start_iso,
            end_iso,
            ingest_type
        )))
        
        raw_data_st_df = None
        if rawcollectiondataServiceTitan:
            print("rawcollectiondata sample doc",rawcollectiondataServiceTitan[0])
            raw_data_st_df =  pd.DataFrame(rawcollectiondataServiceTitan)
            # raw_data_st_df =  rawcollectiondataServiceTitan

        # fetching the engagmentCalls data from engagment calls raw collection
        rawcollectiondataEngagmentCallsRaw = json.loads(dumps(get_engagementCalls_raw_data(
            putil.MONGO_URL,
            putil.MONGO_DBNAME,
            ENGAGEMENT_CALLS_RAW,
            tenantDetail['tenantId'],
            start_iso,
            end_iso,
            ingest_type
        )))
        raw_data_eng_calls_df =None
        if rawcollectiondataEngagmentCallsRaw:
            print("rawcollectiondataEngagmentCallsRaw sample doc",rawcollectiondataEngagmentCallsRaw[0])
            raw_data_eng_calls_df =  pd.DataFrame(rawcollectiondataEngagmentCallsRaw)
            # raw_data_eng_calls_df =  rawcollectiondataEngagmentCallsRaw


        # Set skip_signal to True if raw_data_st_df and raw_data_eng_calls_df are None
        skip_signal_value = True if raw_data_st_df is None or raw_data_eng_calls_df is None else False
        description=None
        if skip_signal_value:
            description="There is no calls data for the provided month"
        
        if raw_data_st_df is None :
            print("There is no servcie titan data available for the provided date!!")
        elif raw_data_eng_calls_df is None:
            print("The engagement calls data is not avilable for the provided date!!")


        parameters_to_pass = {
            "raw_data_st_df":raw_data_st_df,
            "raw_data_eng_calls_df":raw_data_eng_calls_df,
            "skip_signal":skip_signal_value,
            "description":description
            }
        kwargs['ti'].xcom_push(key='get_data_for_inputs_task_args', value=parameters_to_pass)


#########################################################################################################################
 
def insert_data_ingestion_log(dataSourceId,destinationCollectionId,tenantId,clientId,date, category):
    logging.info( DAG_NAME +" : inserting ingestion log for "+tenantId+" , clientId "+clientId+" on "+date)
    client = pymongo.MongoClient(putil.MONGO_URL)
    ram_usage = psutil.virtual_memory().percent
    print(f"Current RAM usage: {ram_usage}%")
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"Current CPU utilization: {cpu_percent}%")
    polarisdb = client[putil.MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[putil.TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceId":dataSourceId,"destinationCollectionId": destinationCollectionId,
            "tenantId":tenantId,"clientId":clientId,"recordDate":date,"ingestionDate":datetime.strftime(TODAYS_DATE, "%Y-%m-%d"),"status":"success", "logType": "Day-wise Ingest", "category": category}
    tenantIngestLogSource.insert_one(dict)
    
#########################################################################################################################
def check_ingestion_log(dataSourceId,destinationCollectionId,tenantId,clientId,date,ingest_type):
    logging.info( DAG_NAME +" : Checking ingestion log for "+tenantId+" , clientId "+clientId+" on "+date)
    client = pymongo.MongoClient(putil.MONGO_URL)

    polarisdb = client[putil.MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[putil.TENANT_INGEST_LOG_COLLECTION]
    if ingest_type == "net_change":
        dict = {"dataSourceId":dataSourceId,"destinationCollectionId": destinationCollectionId,"tenantId":tenantId,"clientId":clientId,"recordDate":date, "ingestionDate":datetime.strftime(TODAYS_DATE, "%Y-%m-%d"),"status":"success", "logType": "Day-wise Ingest"}
    else:
        dict = {"dataSourceId":dataSourceId,"destinationCollectionId": destinationCollectionId,"tenantId":tenantId,"clientId":clientId,"recordDate":date,"status":"success", "logType": "Day-wise Ingest"}    
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    if ingestLogRecord > 0:
        return True
    else:
        return False






# get the records from the extract collection and perform the aggregation to keep the total count of calls without matching engagement calls
def perform_aggregation_on_extract_collection(**kwargs):
        retrieved_params_params_task_config = kwargs['ti'].xcom_pull(key='start_transform_task_inputs', task_ids='start_transform_task')
        retrieved_params_params_task=retrieved_params_params_task_config['config_logs']
        sysDataSourceConfig=retrieved_params_params_task_config["sysDataSourceConfig"]
        ingest_start_date=retrieved_params_params_task['ingest_start_date']
        ingest_end_date=retrieved_params_params_task['ingest_end_date']
        tenant_id=retrieved_params_params_task['tenantId']
        db = putil.get_mongoDb_details()
        #get the records from the extract collection
        # extract_records = putil.get_and_perform_aggreagtion_on_extract_records(RAW_COLLECTION,ingest_start_date,ingest_end_date,tenant_id,sysDataSourceConfig)
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"Current CPU utilization: {cpu_percent}%")
        # added the code to test in airflow reflect
        extract_raw_collection = db["DataServiceTitanBusinessAttributionRaw"]
        extract_aggregated_collection = db["DataServiceTitanBusinessAttributionRawAggregated"]
        engagmentCallsTransform = db['DataEngagementCallsTransformed']
        print("sysDataSourceConfig",sysDataSourceConfig)

        start_date_obj = datetime.strptime(ingest_start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(ingest_end_date, "%Y-%m-%d")
        # Format the strings to remove microseconds
        ISOStartDate = start_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        ISOEndDate = end_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        aggregation_config_str = sysDataSourceConfig[0]['configurations'].get("dataExtractAggregationConf")
        aggregation_config_str = "["+aggregation_config_str+"]"
        aggregation_config_dict= json.loads(re.sub(r'\s+', '', aggregation_config_str))
        print("aggregation_config_dict",type(aggregation_config_dict))
        print(len(aggregation_config_dict))
        match_query = {
            "$match": {
                "entity.tenantId": tenant_id,
                "$expr": {
                    "$and": [
                        {
                            "$gte": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOStartDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        },
                        {
                            "$lt": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": ISOEndDate,
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        }



        pipeline=[]
        if aggregation_config_dict is not None:
            pipeline = [
                        match_query,
                        aggregation_config_dict[0],
                        aggregation_config_dict[1],
                    ]
            # Performing the aggregation
            result_cursor = extract_raw_collection.aggregate(pipeline)

            # Convert the cursor to a list and print the first 5 documents
            result_list = list(result_cursor)

            converted_arr_dic = [dict(document) for document in result_list]
            print("Number of documents aggregate:", len(result_list))
            try:
                # Delete existing records based on the match query
                delete_result=extract_aggregated_collection.delete_many(match_query['$match'])
                # Print the number of records deleted
                num_records_deleted = delete_result.deleted_count
                print(f"{num_records_deleted} record(s) deleted.")
                result_of_inserting = extract_aggregated_collection.insert_many(converted_arr_dic)

                print("Inserted document IDs:", result_of_inserting.inserted_ids)
                extract_aggregated_df = pd.DataFrame(list(extract_aggregated_collection.find({
                                            "$and": [
                                                {"date": {"$gte": start_date_obj, "$lt": end_date_obj}},
                                                {"entity.tenantId": tenant_id}
                                            ]
                                        })))
                print("extract_aggregated_df",extract_aggregated_df)
                engagement_calls_df = pd.DataFrame(list(engagmentCallsTransform.find({
                                        "$and": [
                                            {"date": {"$gte": start_date_obj, "$lt": end_date_obj}},
                                            {"entity.tenantId": tenant_id}
                                        ]
                                    })))
                
                raw_data_clls_dimensions_df = pd.DataFrame(engagement_calls_df['entity'].tolist())
                raw_data_clls_dimensions_df= pd.concat([engagement_calls_df, raw_data_clls_dimensions_df], axis=1)
                raw_data_aggr_dimensions_df = pd.DataFrame(extract_aggregated_df['entity'].tolist())
                merged_df = pd.merge(raw_data_clls_dimensions_df, raw_data_aggr_dimensions_df, on='tenantId', how='outer', suffixes=('_calls', '_aggregated'))
                # Keep both matching and non-matching records based on 'tenantId'
                filtered_df = merged_df.copy()
                # Construct the updated 'entity' field
                def update_entity(row):
                    entity_data = {
                        'tenantId': row['tenantId'],
                        "nDate": row['nDate'],
                        "source": row['source'],
                        "medium": row['medium'],
                        "campaign": row['campaign'],
                        "campaignGroup":row['campaignGroup'],
                        "campaignGroupId": row['campaignGroupId'],
                        "campaignId": row['campaignId'],
                        "channelNameId":row['channelNameId'],
                        "channelId": row['channelId'],
                        "platformId": row['platformId'],
                        "campaignTypeId": row['campaignTypeId'],
                        "channelTypeName": row['channelTypeName'],
                        "channelName": row['channelName'],
                        "platformName": row['platformName'],
                        "campaignTypeName": row['campaignTypeName'],
                        "locationId": row['locationId'],
                        "locationGroupId": row['locationGroupId'],
                        "marketSegmentId": row['marketSegmentId'],
                        "nielsonRegion":row["nielsonRegion"],
                        "dayOfWeek":row['dayOfWeek'],
                        "hourOfDay":row['hourOfDay'],
                        "hour":row["hour"]
                    }
                    return entity_data
                def update_specific_fields(row):
                    specific_fields_data = {
                        'dateClientTz': row['dateClientTz'],
                        'dateLocationTz': row['dateLocationTz'],
                        'dateUTCTz': row['dateUTCTz'],
                    }
                    row.update(specific_fields_data)
                    return row
                filtered_df['entity'] = filtered_df.apply(update_entity, axis=1)
                filtered_df = filtered_df.apply(update_specific_fields, axis=1)
                filtered_df['_id'] = extract_aggregated_df['_id']
                filtered_df = filtered_df.dropna(subset=['_id'])
                filtered_df.reset_index(drop=True, inplace=True)
                ram_usage = psutil.virtual_memory().percent
                print(f"Current RAM usage: {ram_usage}%")
                def update_or_insert_document(row):
                    try:
                        existing_doc = extract_aggregated_collection.find_one({"_id": row["_id"]})

                        if existing_doc:
                            # If document exists, update it
                            extract_aggregated_collection.update_one(
                                {"_id": row["_id"]},
                                {"$set": {
                                    "entity": row["entity"],
                                    "dateClientTz": row["dateClientTz"],
                                    "dateLocationTz": row["dateLocationTz"],
                                    "dateUTCTz": row["dateUTCTz"],
                                    # Other fields for update
                                }}
                            )
                            

                            print(f"Updated document with _id: {row['_id']}")
                        else:
                            # If not, insert a new document
                            extract_aggregated_collection.insert_one(row)
                            print(f"Inserted new document with _id: {row['_id']}")

                    except Exception as e:
                        # Handle other exceptions if needed
                        print(f"Failed to process document with _id: {row['_id']}, Error: {str(e)}")

                # Apply the function to each row in filtered_df
                filtered_df.apply(update_or_insert_document, axis=1)
            except Exception as e:
                print("err inserting documentsssssssss:", str(e))





# function to skip other tasks when the record exists or there is no data 
def decide_to_skip(**kwargs):
    # Retrieve outputs from previous tasks
    retrieved_params = kwargs['ti'].xcom_pull(key='get_data_for_inputs_task_args', task_ids='get_data_for_inputs_task')
    print("retrieved_params",retrieved_params)

    if retrieved_params.get('description'):
        print(retrieved_params.get('description'))
    # Your condition check logic    
    if retrieved_params.get("skip_signal"):
        return False  # Proceed with downstream tasks
    else:
        return True   # Skip downstream tasks

##################################################################################################################################################################################################################################3
# Function to retrieve value from generate_dict task and trigger aggregation dag
def trigger_aggregation(**kwargs):
    ti = kwargs['ti']
    generate_dict_value = ti.xcom_pull(task_ids='create_prams_for_n8n_aggr')
    print("generate_dict_value", generate_dict_value)
    return generate_dict_value




##################################################################################################################################################################################################################################
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

custom_params = {
    "ingest_type": "custom",
    "tenant": "6548c06c5b8b6db6341728f7",
    "ingest_start_date": "2023-11-17",
    "ingest_end_date": "2023-11-18",
}
from airflow.utils.db import provide_session
from airflow.models import XCom



with DAG(
        'ServiceTitanBusinessAttributionTransform',
        default_args=default_args,
        description='Service Titan Business Attribution Transformed Dag',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        concurrency=2,
        catchup=True,
        # params=custom_params,
        tags=['Service Titan Business Attribution'],
) as dag :
    dag.doc_md = __doc__

    def start_transform_task(**kwargs):
        if 'conf' in kwargs['params']: 
            params = ast.literal_eval(kwargs['params']['conf'])
            if 'conf' in params:
                params = ast.literal_eval(params['conf'])
        else:
            params = kwargs['params']
            

        if "ingest_type" in params:
            ingest_type =  params['ingest_type']
            if "tenant" in params:
                tenant_id = params['tenant']
            else:
                tenant_id = ""
                logging.info("Empty TenantId")
            
            ## Get dataFrehsnessDelay
            sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
            dataFreshnessDelay = sysDataSourceConfig[0]['dataFreshnessDelay']       
            tenantDetails  = json.loads(dumps(putil.get_tenant_datasource_details(sysDataSourceConfig[0]["platform"],tenant_id, DAG_NAME)))
            managed=None
            if "managed" in tenantDetails[0]:
                managed = tenantDetails[0]['managed']
            else:
                managed = False
            netChangePeriod=None 
            if "netChangePeriod" in sysDataSourceConfig[0]:
                netChangePeriod = sysDataSourceConfig[0]['netChangePeriod']  
            else:
                netChangePeriod = 0   


            if params['ingest_type'] == "initial":

                tenant = putil.get_tenant(tenant_id) 
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else:     
                    ingest_start_date, ingest_end_date = putil.get_next_batch_dates(tenant[0], sysDataSourceConfig[0], params['ingest_type'], SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT, SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE)
                    
            if params['ingest_type'] == "daily":
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else:                    
                    ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=dataFreshnessDelay), "%Y-%m-%d") 
                    ingest_end_date = datetime.strftime(TODAYS_DATE, "%Y-%m-%d")                

            if params['ingest_type'] == "net_change":
                if "netChangePeriod" not in sysDataSourceConfig[0]:
                    logging.info("net change is not applicable for this adapter")
                    sys.exit()
                   
                netChangePeriod = sysDataSourceConfig[0]['netChangePeriod'] 
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else:     
                    
                    ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=netChangePeriod), "%Y-%m-%d") 
                    ingest_end_date=datetime.strftime(TODAYS_DATE-timedelta(days=1)-timedelta(days=dataFreshnessDelay), "%Y-%m-%d")
                    
            if params['ingest_type'] == "custom":
                ingest_start_date=params['ingest_start_date']
                ingest_end_date=params['ingest_end_date']                
            
            logging.info("Tenant Id: "+str(tenant_id))
            logging.info("Ingest type: "+str(ingest_type))
            logging.info("Ingest start date: "+str(ingest_start_date))
            logging.info("Ingest end date: "+str(ingest_end_date))

            config_logs={
                "ingest_end_date":ingest_end_date,
                "ingest_start_date":ingest_start_date,
                "ingest_type":ingest_type,
                "dataSourceName":sysDataSourceConfig[0]["_id"],
                "tenantId":tenant_id,
                "managed":managed,
                "netChangePeriod":netChangePeriod
            }
            parameters_to_pass = {
                "ingest_start_date":ingest_start_date,
                "ingest_end_date":ingest_end_date,
                "tenant_id":tenant_id,
                "ingest_type":ingest_type,
                "config_logs":config_logs,
                "sysDataSourceConfig":sysDataSourceConfig
            }


            kwargs['ti'].xcom_push(key='start_transform_task_inputs', value=parameters_to_pass)

    def create_prams_for_n8n_aggr_fn(**kwargs):                        
        if 'conf' in kwargs['params']: 
            params = ast.literal_eval(kwargs['params']['conf'])
            
            if 'conf' in params:
                params = ast.literal_eval(params['conf'])
        else:
            params = kwargs['params']
        
        sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
        dataSourceName = sysDataSourceConfig[0]['dataSourceName']
        dataFreshnessDelay = sysDataSourceConfig[0]['dataFreshnessDelay']

        if "ingest_type" in params:
            if "tenant" in params:
                tenant_id = params['tenant']
            else:
                tenant_id = ""
                logging.info("Empty TenantId")
                
            if params['ingest_type'] == "initial":
                tenant = putil.get_tenant(tenant_id) 
                ingest_type=params['ingest_type']
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else: 
                    ingest_start_date, ingest_end_date = putil.get_next_batch_dates(tenant[0], sysDataSourceConfig[0], params['ingest_type'], SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT, SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE)
                
            if params['ingest_type'] == "daily":
                
                ingest_type=params['ingest_type']                
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else:                 
                    ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=dataFreshnessDelay), "%Y-%m-%d") 
                    ingest_end_date=datetime.strftime(TODAYS_DATE-timedelta(days=dataFreshnessDelay), "%Y-%m-%d")                
                
            if params['ingest_type'] == "net_change":
                
                ingest_type=params['ingest_type']
                
                if "netChangePeriod" not in sysDataSourceConfig[0]:
                    logging.info("Net change period not defined or is not applicable for this data source")
                    sys.exit()
                  
                netChangePeriod = sysDataSourceConfig[0]['netChangePeriod'] 
                if 'ingest_start_date' in params and 'ingest_end_date' in params:
                    ingest_start_date = params['ingest_start_date']
                    ingest_end_date = params['ingest_end_date']
                else:                
                    ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=netChangePeriod), "%Y-%m-%d") 
                    ingest_end_date=datetime.strftime(TODAYS_DATE-timedelta(days=1)-timedelta(days=dataFreshnessDelay), "%Y-%m-%d")
                
            if params['ingest_type'] == "custom":
                
                ingest_type=params['ingest_type']
                ingest_start_date=params['ingest_start_date']
                ingest_end_date=params['ingest_end_date']                        
                
        
            request_body= {
            "ingest_type": ingest_type,
            "tenant": params['tenant'],
            "ingest_start_date":ingest_start_date,
            "ingest_end_date":ingest_end_date,
            "data_source_name": dataSourceName
            }
            print("request_body", request_body)
            json_body= json.dumps(request_body)
        
        return json_body


    # default function to excute if any one of the task fails
    def error(**kwargs):
        logging.info("Error task running")
                                
        if 'conf' in kwargs['params']:
            params = ast.literal_eval(kwargs['params']['conf'])
        else:
            params = kwargs['params']      
        if "ingest_type" in params:            
            ingest_type = params['ingest_type']
            if "tenant" in params:
                tenant_id = params['tenant']
            else:
                tenant_id = ""
                logging.info("Empty TenantId")
            ## Get dataFrehsnessDelay
            sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
            
            logging.info("Tenant Id: "+str(tenant_id))
            logging.info("Ingest type: "+str(ingest_type))
            putil.update_pending_status_logs(tenant_id, ingest_type, DATA_SOURCE_NAME, DAG_NAME, "failed")
            raise Exception('Error occurred during the task. Please check the logs for more details.')



    transformed_task = PythonOperator(
            task_id='start_transform_task',
            python_callable=start_transform_task,
            on_failure_callback=putil.error_callback_sns,
            provide_context=True
    )
    getInputsToExtractData = PythonOperator(
            task_id='get_data_for_inputs_task',
            python_callable=get_data_for_inputs,
            on_failure_callback=putil.error_callback_sns,
            provide_context=True,
    )
    matchEngagementandSerciceTitanData =  PythonOperator(
            task_id='match_st_data_with_eng_data_task',
            python_callable=match_st_data_with_eng_data,
            on_failure_callback=putil.error_callback_sns,
            provide_context=True,
    )
    # this task is being added to get the calls total from the extract dag
    performExtractAggregation=PythonOperator(
        task_id='perform_extract_aggregation_task',
        python_callable=perform_aggregation_on_extract_collection,
        on_failure_callback=putil.error_callback_sns,
        provide_context=True,
        dag=dag
    )
    create_prams_for_n8n_aggr = PythonOperator(
        task_id='create_prams_for_n8n_aggr',
        python_callable=create_prams_for_n8n_aggr_fn,
        on_failure_callback=putil.error_callback_sns,
        provide_context=True,
        dag=dag
    )
    # api_trigger_dependent_dag= SimpleHttpOperator(
    #     task_id="api_trigger_dependent_dag",
    #     http_conn_id='n8n-webhook',
    #     endpoint='webhook/preaggregate',
    #     method='POST',
    #     headers={'Content-Type': 'application/json'},
    #     data = "{{ task_instance.xcom_pull(task_ids='create_prams_for_n8n_aggr') }}",
    #     dag=dag
    # )

    # Task to retrieve value from generate_data task and trigger aggregation dag
    trigger_aggregation_task = PythonOperator(
        task_id='trigger_aggregation_task',
        python_callable=trigger_aggregation,
        on_failure_callback=putil.error_callback_sns,
        provide_context=True,
        dag=dag
    )


    # task to trigger aggregatin dag
    trigger_aggregation_dag = TriggerDagRunOperator(
        task_id="trigger_aggregation_dag",
        trigger_dag_id="ServiceTitanBusinessAttributionAggregation",
        dag=dag,
        conf="{{ task_instance.xcom_pull(task_ids='trigger_aggregation_task') }}"
    )

    # taks to skip the other tasks when there is no data or ingest log exists
    # Define ShortCircuitOperator for conditional execution of downstream tasks
    skip_downstream_tasks = ShortCircuitOperator(
        task_id='skip_downstream_tasks',
        python_callable=decide_to_skip,
        provide_context=True,
        dag=dag
    )


    error_task = PythonOperator(
        task_id='error',
        python_callable=error,
        dag=dag,
        trigger_rule = "one_failed"
    )
    
    # transformed_task >> getInputsToExtractData >> skip_downstream_tasks >>[ matchEngagementandSerciceTitanData , performExtractAggregation,create_prams_for_n8n_aggr , api_trigger_dependent_dag ]>> error_task
   
   
   

   
    # Define task dependencies
    transformed_task >> getInputsToExtractData >> skip_downstream_tasks

    # Branching for conditional execution
    skip_downstream_tasks >> matchEngagementandSerciceTitanData

    matchEngagementandSerciceTitanData >> performExtractAggregation
    performExtractAggregation >> create_prams_for_n8n_aggr
    create_prams_for_n8n_aggr >> trigger_aggregation_task >> trigger_aggregation_dag

    # Define the error task to run only if any of the tasks fail
    [matchEngagementandSerciceTitanData, performExtractAggregation, create_prams_for_n8n_aggr, trigger_aggregation_task,  trigger_aggregation_dag] >> error_task