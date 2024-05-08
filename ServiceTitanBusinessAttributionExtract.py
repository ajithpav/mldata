
# new code with pandas

import json
from datetime import datetime 
from datetime import timedelta
from datetime import date
from textwrap import dedent
import time
import requests
import pymongo
from bson.json_util import dumps
import logging
from airflow.models import Variable
import re
import sys
import polaris_util as putil
import ast
from airflow import DAG
from airflow.operators.python import PythonOperator,ShortCircuitOperator,BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

import pandas as pd
import uuid
import psutil




DATA_SOURCE_NAME = "ServiceTitanBusinessAttribution"
DAG_NAME = "ServiceTitanBusinessAttributionExtract"
TODAYS_DATE = date.today()
ENGAGEMENT_CALLS_RAW = "DataEngagementCallsRaw"
RAW_COLLECTION="DataServiceTitanBusinessAttributionRaw"
ACCESSTOKEN = ""

try:
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE = Variable.get("SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE")
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT = Variable.get("SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT")
except KeyError:
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE = None
    SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT = None
   
putil.sentry_error_finder()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)



# fn to check the ingestion logs
def check_ingestion_log(dataSourceName, destinationCollectionId, tenantId, ingest_start_date, ingest_type):
    print("dataSourceId,destinationCollectionId,tenantId,startDateFormatted,ingest_type", dataSourceName, destinationCollectionId, tenantId, ingest_start_date, ingest_type)
    logging.info(DAG_NAME + " : Checking ingestion log for " + tenantId + " on " + str(ingest_start_date))
    # Establish MongoDB connection
    client = pymongo.MongoClient(putil.MONGO_URL)
    polarisdb = client[putil.MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[putil.TENANT_INGEST_LOG_COLLECTION]
    
    # Define the query based on ingest_type
    query = {
        "destinationCollectionId": destinationCollectionId,
        "tenantId": tenantId,
        "status": "success",
    }
    if ingest_type == "net_change":
        # returning to False  if net chnage is the case any how we need to update the data
        return False
    else:
        query["dataSourceName"] = dataSourceName
        query["ingestionStartDate"] =  ingest_start_date
        query["ingestType"]=ingest_type
    print("query",query)
    # Count the documents based on the query
    ingestLogRecord = tenantIngestLogSource.count_documents(query)
    print("ingestLogRecord", ingestLogRecord)
    
    # Return True if documents exist, False otherwise
    return ingestLogRecord > 0


# function to get the access token
def get_access_token(client_id, client_secret):
    print("Getting Access token!!!!")
    token_url = "https://auth.servicetitan.io/connect/token"
    data = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    headers={"Content-Type": "application/x-www-form-urlencoded","Accept": "application/json"}
    auth_response = requests.post(token_url, data=data, headers=headers)
    auth_response_json = json.loads(auth_response.text)
    # print("auth_response_json",auth_response_json)
    access_token = auth_response_json['access_token']
    global ACCESSTOKEN
    ACCESSTOKEN = access_token




#function to get the calls
def get_calls_data(tenant, start_date, end_date, client_id, client_secret):
    attempts = 1
    backoff_in_seconds = 1
    page = 1
    combined_response = {}

    while True:
        try:
            logging.info(f"{DAG_NAME} DAG: Retrieving data for client tenant Id: {tenant} from {start_date} to {end_date}")
            headers = {"Authorization": f"Bearer {ACCESSTOKEN}", "ST-App-Key": "ak1.bat5zwmbb5ts5ym5sqgj0s86u"}
            calls_url = f'https://api.servicetitan.io/telecom/v3/tenant/{tenant}/calls?createdOnOrAfter={start_date}&createdBefore={end_date}&page={page}&pageSize=5000'

            response = requests.get(calls_url, headers=headers)
            if response.status_code != 200 or response.text is None:
                raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")

            response_data = json.loads(response.text)
            if not combined_response:
                combined_response = response_data
            else:
                combined_response['data'].extend(response_data['data'])

            if not response_data['hasMore']:
                return combined_response

            if str(response.status_code) == "401":
                get_access_token(client_id, client_secret)

            sleep = backoff_in_seconds * 2 ** (attempts - 1)
            logging.info(f"Retrying request (attempt #{attempts}) in {sleep} seconds...")
            time.sleep(sleep)
            attempts += 1
            page += 1

        except Exception as e:
            logging.info(f"{DAG_NAME} : Error Response {str(e)}")
            if str(response.status_code) == "401":
                get_access_token(client_id, client_secret)
            if attempts > 2:
                return {"errorCode": "Return Error"}

            sleep = backoff_in_seconds * 2 ** (attempts - 1)
            logging.info(f"Retrying request (attempt #{attempts}) in {sleep} seconds...")
            time.sleep(sleep)
            attempts += 1





#########################################################################################################################
def get_invoices_data(tenant, job_number, client_id, client_secret):    
    
    attempts = 1
    backoff_in_seconds=1    
    
    while True: 
        try: 
                        
            headers = {"Authorization": "Bearer "+ACCESSTOKEN, "ST-App-Key": "ak1.bat5zwmbb5ts5ym5sqgj0s86u"}
            invoices_url = 'https://api.servicetitan.io/accounting/v2/tenant/'+tenant+'/invoices?jobNumber='+job_number
            response = requests.get(invoices_url, headers=headers)
            # if headers and invoices_url:
            #     print("headers and invoices_url:",str(headers),str(invoices_url))       
            if response.status_code != 200:
                raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")
                
            if response.text == None :
                raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")            
                    
            response = json.loads(response.text)                                                
                           
            return response
        except RuntimeError as err:
            logging.info( DAG_NAME +" : Error Response "+str(err))
            if str(response.status_code) == "401":
                get_access_token(client_id, client_secret)             
            if attempts > 2:                
                return {"errorCode": response.status_code}
                
            sleep = backoff_in_seconds * 2 ** (attempts - 1)
            print(f"Retrying request (attempt #{attempts}) in {sleep} seconds...")
            time.sleep(sleep)
            attempts += 1



# Function to get project data
def get_project_data(**kwargs):
    print("Getting the project details!!!")
    retrieved_params = kwargs['ti'].xcom_pull(key='enhance_records_task', task_ids='enhance_records')
    project_ids = retrieved_params['projectIds']
    get_access_token(retrieved_params["id_for_access_token"], retrieved_params["client_secret"])

    batch_size = 50
    batches = [project_ids[i:i + batch_size] for i in range(0, len(project_ids), batch_size)]
    combined_response = []  # To store combined responses

    for batch in batches:
        ids_combined = ','.join(str(project_id) for project_id in batch)
        attempts = 1
        backoff_in_seconds = 1
        page = 1

        while True:
            try:
                headers = {
                    "Authorization": "Bearer " + ACCESSTOKEN,
                    "ST-App-Key": "ak1.bat5zwmbb5ts5ym5sqgj0s86u"
                }

                project_url = f"https://api.servicetitan.io/jpm/v2/tenant/{retrieved_params['tenantId']}/projects?ids={ids_combined}&includetotal=true&page={page}&pageSize=5000"

                response = requests.get(project_url, headers=headers)

                if response.status_code != 200:
                    raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")

                if response.text == None:
                    raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")

                response = json.loads(response.text)
                combined_response.extend(response['data'])  # Combine responses

                if not response['hasMore']:
                    break  # Exit the loop when there are no more results
                
                page += 1  # Move to the next page for more results

            except RuntimeError as err:
                logging.info(DAG_NAME + " : Error Response " + str(err))
                if str(response.status_code) == "401":
                    get_access_token(retrieved_params['tenantId'], retrieved_params['client_secret'])

                if attempts > 2:
                    return {"errorCode": response.status_code}

                sleep = backoff_in_seconds * 2 ** (attempts - 1)
                print(f"Retrying request (attempt #{attempts}) in {sleep} seconds...")
                time.sleep(sleep)
                attempts += 1

    parameters_to_pass = {
        "UniqueProjectIdsData":combined_response,
        "tenantId":retrieved_params["tenantId"], 
    }
    kwargs['ti'].xcom_push(key='project_task', value=parameters_to_pass)
    return combined_response




#getting the job details using id:
def get_jobs_data(job_number,client_tenant_id,id_for_access_token,client_secret):
    attempts = 1
    backoff_in_seconds=1    
    # print("job_number,client_tenant_id",job_number,client_tenant_id)
    # get_access_token(id_for_access_token, client_secret)
    while True:
        try: 
                        
            headers = {"Authorization": "Bearer "+ACCESSTOKEN, "ST-App-Key": "ak1.bat5zwmbb5ts5ym5sqgj0s86u"}
            jobs_url = 'https://api.servicetitan.io/jpm/v2/tenant/'+str(client_tenant_id)+'/jobs?number='+str(job_number)
            response = requests.get(jobs_url, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")
                
            if response.text == None:
                raise RuntimeError(f"HTTP Response Code {response.status_code} received from server.")
                    
            response = json.loads(response.text)                                                
                           
            return response
        except RuntimeError as err:
            logging.info( DAG_NAME +" : Error Response "+str(err))
            if str(response.status_code) == "401":
                get_access_token(id_for_access_token, client_secret)             
            if attempts > 2:                
                return {"errorCode": response.status_code}
               
            sleep = backoff_in_seconds * 2 ** (attempts - 1)
            print(f"Retrying request (attempt #{attempts}) in {sleep} seconds...")
            time.sleep(sleep)
            attempts += 1 



# function to mep the project data with the calls data using projet id
def merge_project_details(**kwargs):
    retrieved_params_from_proj_task = kwargs['ti'].xcom_pull(key='project_task', task_ids='project_task')
    retrieved_params_params_task_par = kwargs['ti'].xcom_pull(key='extract_task_params', task_ids='extract_service_titan_business')
    retrieved_params_from_enhance_task = kwargs['ti'].xcom_pull(key='enhance_records_task', task_ids='enhance_records')
    client_id=retrieved_params_params_task_par["client_id"]
    external_batch_number=retrieved_params_params_task_par["external_batch_number"]
    srcDataFromServiceTitan=retrieved_params_from_enhance_task['srcDataFromServiceTitan']
    retrieved_params_params_task=retrieved_params_params_task_par['config_logs']
    # df_uniq_proj_data = pd.DataFrame(retrieved_params_from_proj_task['UniqueProjectIdsData'])
    #no need to convert into data frame beacuse we sending it as df from xcom
    # df_uniq_src_data = pd.DataFrame(srcDataFromServiceTitan)
    df_uniq_src_data = srcDataFromServiceTitan
    ingest_type=retrieved_params_params_task['ingest_type']
    ingest_start_date=retrieved_params_params_task['ingest_start_date']
    ingest_end_date=retrieved_params_params_task['ingest_end_date']
    tenant_id=retrieved_params_params_task['tenantId']    
    sysDataSourceConfig= retrieved_params_params_task_par['sysDataSourceConfig']
    category=None
    ingestionSchedule=None
    if "category" in sysDataSourceConfig[0]:
        category = sysDataSourceConfig[0]['category']  
    else:
        category = "Business"   
    if "ingestionSchedule" in sysDataSourceConfig[0]:
            if ("dailyLoad" in sysDataSourceConfig[0]["ingestionSchedule"]) and (ingest_type == "daily"):
                    ingestionSchedule = sysDataSourceConfig[0]["ingestionSchedule"]['dailyLoad']
            elif ("netChange" in sysDataSourceConfig[0]["ingestionSchedule"]) and (ingest_type == "net_change"):
                    ingestionSchedule = sysDataSourceConfig[0]["ingestionSchedule"]['netChange']
            else:
                    ingestionSchedule = ""
    else:
        ingestionSchedule = ""

    # Merge the project details based on the project IDs
    print("Length df_uniq_src_data ",len(df_uniq_src_data))



    #processed_invoice_ids = set()
    # Function to fetch job data from API and update dimensions
    def update_dimensions_with_job_and_invoice_data(row, invoice_list, job_list, tenant_id):
        # print("job_----date >>>>>>>>>>>>>>>>.")
        job_number = row['dimensions']['jobNumber']  # Assuming jobNumber is present in dimensions
        processed_invoice_ids = set()
        # Fetch job data from the API

        if job_number is not None:
            # Fetch job data
            # print("---",job_number, retrieved_params_from_enhance_task['tenantId'], retrieved_params_from_enhance_task["id_for_access_token"], retrieved_params_from_enhance_task["client_secret"])
            jobs_data = get_jobs_data(job_number, retrieved_params_from_enhance_task['tenantId'], retrieved_params_from_enhance_task["id_for_access_token"], retrieved_params_from_enhance_task["client_secret"])
            # Update dimensions with jobStatus if available in the API response
            if jobs_data and "data" in jobs_data and jobs_data.get('data'):
                print("updating the inovice id - Job Number:", job_number)
                jobs_data['data'][0]['date'] = row['date']
                jobs_data['data'][0]['tenantId'] = tenant_id
                job_list.extend(jobs_data['data'])
                # Parse string to datetime object
                jobs_created_on = datetime.strptime(jobs_data['data'][0].get('createdOn')[0:10], "%Y-%m-%d")
                lead_call_created_on = datetime.strptime(row['leadCall']['createdOn'][0:10], "%Y-%m-%d")
                # Extract the date part
                jobs_created_on = jobs_created_on.date()
                lead_call_created_date = lead_call_created_on.date()
                jobs_data = jobs_data['data']
                if jobs_data:
                    if jobs_created_on >= lead_call_created_date:
                        row['dimensions']['jobStatus'] = jobs_data[0].get('jobStatus')
                        row['dimensions']['jobTypeId'] = jobs_data[0].get('joTypeId')
                        row['dimensions']['completedOn'] = jobs_data[0].get('completedOn')
                        row['dimensions']['JobCreatedOn'] = jobs_data[0].get('createdOn')
                        invoices_data = get_invoices_data(retrieved_params_from_enhance_task['tenantId'], job_number,
                                                          retrieved_params_from_enhance_task["id_for_access_token"],
                                                          retrieved_params_from_enhance_task["client_secret"])
                        # print("invoices_data",invoices_data)
                        # Update dimensions with invoice data
                        if invoices_data and "data" in invoices_data:
                            invoices_data = invoices_data['data']
                            if invoices_data:
                                for invoice in invoices_data:
                                    invoice['date'] = row['date']
                                    invoice['tenantId'] = tenant_id
                                    invoice_id = invoice['id']
                                    # Check if the invoice ID is unique and total is greater than zero
                                    if invoice_id not in processed_invoice_ids and float(invoice['subTotal']) > 0:
                                        processed_invoice_ids.add(invoice['id'])  # Add to the set of processed IDs
                                        row['dimensions']['invoiceId'] = str(invoice['id'])
                                        row['dimensions']['jobType'] = invoice['job']['type']
                                        row['dimensions']['businessUnitId'] = invoice['businessUnit']['id']
                                        row['dimensions']['businessUnitName'] = invoice['businessUnit']['name']
                                        # print("Invoice Id:",invoice_id,"invoice amount :",invoice['subTotal'])
                                        # Update metrics
                                        metrics = {
                                            'revenue': float(invoice['subTotal']) if float(
                                                invoice['subTotal']) and not pd.isna(invoice['subTotal']) else  0.00
                                        }
                                        row['metrics'] = metrics
                                invoice_list.extend(invoices_data)

                            else:
                                row['metrics'] = {'revenue': 0.0}
                    else:
                        return job_fillter(row)
                else:
                    return job_fillter(row)
        else:
            return job_fillter(row)
        return row

    def job_fillter(row):
        row['dimensions']['jobNumber'] = None
        row['dimensions']['invoiceId'] = None
        row['dimensions']['jobType'] = None
        row['dimensions']['businessUnitId'] = None
        row['dimensions']['businessUnitName'] = None
        row['dimensions']['jobStatus'] = None
        row['dimensions']['jobTypeId'] = None
        row['dimensions']['JobCreatedOn'] = None
        row['dimensions']['completedOn'] = None
        row['metrics'] = {'revenue': 0.0}
        return row
    # Set to keep track of processed invoice IDs
    invoice_list = []
    job_list = []
    # Update dimensions with job and invoice data for each record
    df_uniq_src_data_enhanced = df_uniq_src_data.apply(update_dimensions_with_job_and_invoice_data, args=[invoice_list, job_list, tenant_id],  axis=1)
    job_collection = 'DataServiceTitanBusinessAttributionJobsRaw'
    invoice_collection = 'DataServiceTitanBusinessAttributionInvoiceRaw'
    client = pymongo.MongoClient( putil.MONGO_URL)
    polarisdb = client[putil.MONGO_DBNAME]
    job_collection_data = polarisdb[job_collection]
    invoice_collection_data = polarisdb[invoice_collection]
    if job_list:
        job_collection_data.insert_many(job_list)
    if invoice_list:
        invoice_collection_data.insert_many(invoice_list)
    del job_list, invoice_list
    # Convert df_uniq_src_data to a list of dictionaries
    data_with_proj_details = df_uniq_src_data_enhanced.to_dict('records')
    if data_with_proj_details is not None:
        putil.insert_servictian_calls_src_data_into_mongo_raw_collection(data_with_proj_details,RAW_COLLECTION,ingest_type,external_batch_number,client_id,tenant_id)
        putil.insert_ingestion_summary_log_with_config(retrieved_params_params_task_par['config_logs'],RAW_COLLECTION,category,ingestionSchedule)


    # Pushing the updated df_uniq_src_data to XCom
    kwargs['ti'].xcom_push(key='updated_src_data', value=df_uniq_src_data_enhanced.to_json())




# adding dimesnions to the extrcated records
def create_dimensions(row):
    campaign = row['leadCall']['campaign']

    dimensions = {
        'callId': str(row['leadCall']['id']),
        'agent': str(row['leadCall']['agent']),
        'callType': str(row['leadCall']['callType']),
        'duration': str(row['leadCall']['duration']),
        'from': str(row['leadCall']['from']),
        'to': str(row['leadCall']['to']),
        'voiceMailUrl': str(row['leadCall']['voiceMailUrl']),
        'campaign': str(row['leadCall']['campaign']),
        'disposition': row['leadCall']['callType'],
        'service': row['type']['name'] if ('type' in row and row['type'] and 'name' in row['type']) else None,
        'direction': row['leadCall']['direction'],
        'receivedOn': row['leadCall']['receivedOn'],
        'callDate': datetime.strptime(row['leadCall']['receivedOn'][:19], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d"),
        'callTime': datetime.strptime(row['leadCall']['receivedOn'][:19], "%Y-%m-%dT%H:%M:%S").strftime("%H:%M:%S"),
        'hourOfDay': datetime.strptime(row['leadCall']['receivedOn'][:19], "%Y-%m-%dT%H:%M:%S").strftime("%H"),
        'callDuration': row['leadCall']['duration'],
        'fromPhoneNumber': row['leadCall']['from'],
        'toPhoneNumber': row['leadCall']['to'],
        'customerPhoneNumber': None,
        'projectId':row['projectId'],
        'reason':row['leadCall']['reason']['name'] if row['leadCall']['reason'] is not None else None,
        'campaignCategory': campaign.get('category')['name'] if campaign and campaign.get('category') else campaign.get('category') if campaign else None,
        'campaignName':campaign['name'] if campaign else campaign,
        'jobNumber':row['jobNumber'],
        'jobType': None,
        'businessUnitId':None,
        'businessUnitName': None,
        'revenue': 0.0,
        'invoiceId': None
    }
    customer = row['leadCall']['customer']
    if customer:
        dimensions['customerId'] = customer.get('id')
        dimensions['customerType'] = customer.get('type')
        dimensions['customerName'] = customer.get('name')
        dimensions['customerEmail'] = customer.get('email')
        address = customer.get('address', {})
        dimensions['customerAddress'] = address.get('streetAddress')
        dimensions['customerUnit'] = address.get('unit')
        dimensions['customerCity'] = address.get('city')
        dimensions['customerCountry'] = address.get('country')
        dimensions['customerState'] = address.get('state')
        dimensions['customerZip'] = address.get('zip')

        phone_settings = customer.get('phoneSettings')
        if phone_settings is not None and phone_settings != []:
            dimensions['customerPhoneNumber'] = phone_settings[0]['phoneNumber']


    return dimensions  # Return the 'dimensions' dictionary directly




# def get_and_enhance_records(external_batch_number, client_id,tenantId,client_tenant_id,client_secret):
def get_and_enhance_records(**kwargs):
    print("Enhancing the records (Adding Entity fields to the source data!)")
    print("kwargs",kwargs)
    retrieved_params = kwargs['ti'].xcom_pull(key='extract_task_params', task_ids='extract_service_titan_business')
    retrieved_params_params_task_config = kwargs['ti'].xcom_pull(key='extract_task_params', task_ids='extract_service_titan_business')
    retrieved_params_params_task=retrieved_params_params_task_config['config_logs']
    external_batch_number=retrieved_params["external_batch_number"]
    client_id=retrieved_params["client_id"]
    tenantId=retrieved_params["tenantId"]
    client_tenant_id=retrieved_params["client_tenant_id"]
    client_secret=retrieved_params["client_secret"]
    ingest_type=retrieved_params_params_task['ingest_type']
    ingest_start_date=retrieved_params_params_task['ingest_start_date']
    ingest_end_date=retrieved_params_params_task['ingest_end_date']
    tenant_id=retrieved_params_params_task['tenantId']
    

    # start_date_obj = datetime.strptime(ingest_start_date, "%Y-%m-%d")
    # end_date_obj = datetime.strptime(ingest_end_date, "%Y-%m-%d")
    # start_date = ingest_start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    # end_date = ingest_end_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    print('client_tenant_id', client_tenant_id)
    start_date = datetime.strptime(ingest_start_date, "%Y-%m-%d")
    end_date = datetime.strptime(ingest_end_date, "%Y-%m-%d")
    records = putil.get_service_titan_data_from_src_collection(start_date, end_date, client_tenant_id)
    ram_usage = psutil.virtual_memory().percent
    print(f"Current RAM usage: {ram_usage}%")
    convertedProjectIds=[]
    if records:
        # Enhancing the data using Pandas DataFrame
        records_df = pd.DataFrame(records)
        records_df['un_formated_date'] = records_df['leadCall'].apply(lambda x: pd.to_datetime(x.get('receivedOn')).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00') if isinstance(x, dict) and x.get('receivedOn') else '')
        records_df['date'] = records_df['un_formated_date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f+00:00") if isinstance(x, str) else x)
        records_df['lastUpdated']=str(datetime.now())  
        # Add 'entity' field
        records_df['entity'] = records_df.apply(lambda row: {
            "tenantId": str(tenantId),
            "date":  row['date'].to_pydatetime(),  # Accessing date from the row
            # "clientId": str(client_i(d),
            # "clientTenantId": str(client_tenant_id)
        }, axis=1)
        records_df['dimensions'] = records_df.apply(create_dimensions, axis=1)
        records_df.drop(columns=['id'], inplace=True)
        # print("df...",records_df['dimensions'].values)
        unique_proj_ids  = records_df['dimensions'].str['projectId'].unique()
        convertedProjectIds = unique_proj_ids.astype(str).tolist()

        # convertedProjectIds = {str(value): str(value) for value in insertunique_proj_ids.tolist()}
        if convertedProjectIds:
            print("convertedProjectIds",convertedProjectIds,)

        # Response_records = df.to_dict('records')
        Response_records = records_df.to_dict('records')
        # if Response_records:
        #     putil.insert_servictian_calls_src_data_into_mongo_raw_collection(Response_records,"DataServiceTitanBusinessAttributionRaw",ingest_type,external_batch_number,client_id,tenant_id)
        if Response_records is not None:
            parameters_to_pass = {
            # "projectIds":convertedProjectIds,
            "tenantId":client_tenant_id,
            "id_for_access_token":client_id,
            "client_secret":client_secret,
            "srcDataFromServiceTitan": records_df
            }
            kwargs['ti'].xcom_push(key='enhance_records_task', value=parameters_to_pass)




#function to being called from the task 1 to pull the call data from the api
def extract_service_titan_business(**kwargs):
    if 'conf' in kwargs['params']:
        params = ast.literal_eval(kwargs['params']['conf'])
    else:
        params = kwargs['params'] 
        
    if "ingest_type" in params:
        # defing variable for ingestion type
        ingest_type=params['ingest_type']
        ingestionStartDate = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ')
        if "tenant" in params:
            tenant_id = params['tenant']
        else:
            tenant_id = ""
            logging.info("Empty TenantId")
            
        # Get dataFrehsnessDelay
        sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
        dataFreshnessDelay = sysDataSourceConfig[0]['dataFreshnessDelay']   
        netChangePeriod=None 
        if "netChangePeriod" in sysDataSourceConfig[0]:
            netChangePeriod = sysDataSourceConfig[0]['netChangePeriod']  
        else:
            netChangePeriod = 0 


        if ingest_type== "initial":
            if 'ingest_start_date' in params and 'ingest_end_date' in params:
                ingest_start_date = params['ingest_start_date']
                ingest_end_date = params['ingest_end_date']
            else:            
                tenant = putil.get_tenant(tenant_id)                              
                if putil.check_initial_ingestion_summary_log(sysDataSourceConfig[0]["_id"], tenant_id, ingest_type):
                    logging.info("Historical ingestion is already done!")
                ingest_start_date, ingest_end_date = putil.get_next_batch_dates(tenant[0], sysDataSourceConfig[0], params['ingest_type'], 
                                                                SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_UNIT, SERVICE_TITAN_BUSINESS_ATTRIBUTION_HISTORICAL_BATCH_SIZE)
        elif ingest_type == "daily":
            if 'ingest_start_date' in params and 'ingest_end_date' in params:
                ingest_start_date = params['ingest_start_date']
                ingest_end_date = params['ingest_end_date']
            else:
                ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=dataFreshnessDelay), "%Y-%m-%d") 
                ingest_end_date = datetime.strftime(TODAYS_DATE, "%Y-%m-%d")
        elif ingest_type == "net_change":
            if "netChangePeriod" not in sysDataSourceConfig[0]:
                logging.info("Net change period not defined or is not applicable for this adapter")
            netChangePeriod = sysDataSourceConfig[0]['netChangePeriod'] 
            if 'ingest_start_date' in params and 'ingest_end_date' in params:
                ingest_start_date = params['ingest_start_date']
                ingest_end_date = params['ingest_end_date']
            else:
                ingest_start_date=datetime.strftime(TODAYS_DATE-timedelta(days=netChangePeriod), "%Y-%m-%d") 
                ingest_end_date=datetime.strftime(TODAYS_DATE-timedelta(days=1)-timedelta(days=dataFreshnessDelay), "%Y-%m-%d")     
        elif ingest_type == "custom":
            ingest_type=params['ingest_type']
            ingest_start_date=params['ingest_start_date']
            ingest_end_date=params['ingest_end_date']                
        
        logging.info("Tenant Id: "+str(tenant_id))
        logging.info("Ingest type: "+str(ingest_type))
        logging.info("Ingest start date: "+str(ingest_start_date))
        logging.info("Ingest end date: "+str(ingest_end_date))
        ram_usage = psutil.virtual_memory().percent
        print(f"Current RAM usage: {ram_usage}%")
        if ingest_start_date and ingest_end_date and tenant_id:
                sysDataSourceConfig = json.loads(dumps(putil.get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
    tenantDetails  = json.loads(dumps(putil.get_tenant_datasource_details(sysDataSourceConfig[0]["platform"],tenant_id, DAG_NAME)))
    if len(tenantDetails) != 0:
        tenantDetail = tenantDetails[0]
        customer_ids = tenantDetail['accessInfo']
        managed=None
        if "managed" in tenantDetails[0]:
            managed = tenantDetails[0]['managed']
        else:
            managed = False       
        startDateFormatted = datetime.strptime(ingest_start_date, "%Y-%m-%d")
        endDateFormatted = datetime.strptime(ingest_end_date, "%Y-%m-%d")
        client_id=None
        check_tenant_subscritption_st =None
        if tenant_id != "" or tenant_id is not None:
            platform="ServiceTitan"
            print("checking tenant subscription!!!")
            check_tenant_subscritption_st = putil.check_tenant_subscritption_with_service_titan(tenant_id,platform)
        if check_tenant_subscritption_st == None or check_tenant_subscritption_st == False:
                print(f"skipping the dag instance since the tenant :{tenant_id} is not subscribed to the Service titan!!")
                parameters_to_pass = {
                    "skip_signal":True,
                    "description":str("skipping the dag instance since the tenant :{tenant_id} is not subscribed to the Service titan!!")
                }
                kwargs['ti'].xcom_push(key='extract_task_params', value=parameters_to_pass)
        elif check_ingestion_log(sysDataSourceConfig[0]["dataSourceName"],sysDataSourceConfig[0]["destinationCollections"]['collectionRaw'],tenantDetail['tenantId'],ingest_start_date,ingest_type):
                logging.info( DAG_NAME +" : Service Titan record exists for  "+ingest_start_date)
                parameters_to_pass = {
                    "skip_signal":True,
                     "description":str("Service Titan record exists for  "+ingest_start_date)
                }
                kwargs['ti'].xcom_push(key='extract_task_params', value=parameters_to_pass)
        else:
            for customer_id in customer_ids:
                client_id = customer_id["clientId"]
                client_secret = customer_id["clientSecret"]
                client_tenant_id = customer_id["tenantId"]
                get_access_token(client_id, client_secret)

                external_batch_number = None
                callsDataFromApi = get_calls_data(client_tenant_id,str(startDateFormatted.date()),str(endDateFormatted.date()), client_id, client_secret)
                print("Sample Records for Service titan calls :",callsDataFromApi)
                if callsDataFromApi is not None and 'data' in callsDataFromApi and callsDataFromApi.get('data'):
                    data =callsDataFromApi['data']
                    print("Length of Call data",len(data))
                    # Generate a unique batch number (UUID)
                    external_batch_number = str(uuid.uuid4())[:8]
                    # external_batch_number='98ecda61'
                    # Convert data to DataFrame
                    df = pd.DataFrame(data)
                    # Add externally provided batch number and tenant ID
                    # df['batchDate'] = startDateFormatted
                    df['clientId'] = client_tenant_id
                    def strecivedon(row):
                        row['date'] =  pd.to_datetime(row.get('receivedOn'))
                        return row['date']
                    df['date'] = df['leadCall'].apply(strecivedon)
                    # print("df['date']", df['date'])
                    batch_detail = {
                                # 'batchNumber': external_batch_number,
                                'clientId': client_tenant_id,
                                # 'batchDate':startDateFormatted,
                                'lastUpdated':str(datetime.now())
                                }
                    # Convert DataFrame back to list of dictionaries
                    modified_data = df.to_dict('records')
                    print("Length of Modified_data",len(modified_data))
                    if modified_data is not None:
                        print("started ingesteing to  ST source collection ",datetime.now())
                        result=putil.insert_servictian_calls_src_data_into_mongo(modified_data,ingest_start_date,ingest_end_date, client_tenant_id)
                        print("Ingestion completed for ST source calls data",datetime.now())
                        # if result:
                        #     get_and_enhance_records(external_batch_number, client_id,tenantId,client_tenant_id,client_secret)


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
                        "external_batch_number":external_batch_number,
                        "client_id":client_id,
                        "tenantId":tenant_id,
                        "client_tenant_id":client_tenant_id,
                        "client_secret":client_secret,
                        "config_logs":config_logs,
                        "sysDataSourceConfig":sysDataSourceConfig[0]["_id"],
                        "skip_signal":False
                    }
                    kwargs['ti'].xcom_push(key='extract_task_params', value=parameters_to_pass)
                else:
                    logging.info(DAG_NAME + " : Service Titan Call data is Empty")
                    parameters_to_pass = {
                        "skip_signal": True,
                        "description": str("Service Titan Calls Api data is Empty")
                    }
                    kwargs['ti'].xcom_push(key='extract_task_params', value=parameters_to_pass)
    else:
        logging.info(DAG_NAME + " : Service Titan Tenants data source Empty")
        parameters_to_pass = {
            "skip_signal": True,
            "description": str("Service Titan Tenants data source Empty")
        }
        kwargs['ti'].xcom_push(key='extract_task_params', value=parameters_to_pass)


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
        

# function to skip other tasks when the record exists or there is no data    
def decide_to_skip(**kwargs):
    # Retrieve outputs from previous tasks
    retrieved_params = kwargs['ti'].xcom_pull(key='extract_task_params', task_ids='extract_service_titan_business')

    if retrieved_params.get('description'):
        print(retrieved_params.get('description'))
    # Your condition check logic    
    if retrieved_params.get("skip_signal"):
        return False  # Proceed with downstream tasks
    else:
        return True   # Skip downstream tasks
    



# onfiguration for dag:
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
	"tenant": "65b0aad625acc643eb5dbfc8",
	"ingest_start_date": "2023-12-01",
	"ingest_end_date": "2023-12-02",
}

with DAG(
        'ServiceTitanBusinessAttributionExtract',
        default_args=default_args,
        description='''supported ingest types:
                        custom
                        net_change
                        initial
                        daily
                        Time period:
                        one day: 7-8 minutes
                        one month: 45-50 minutes''',
        schedule_interval=None,
        #params=custom_params,
        start_date=datetime(2021, 1, 1),
        max_active_runs=4,
        concurrency=4,
        catchup=True,
        tags=['ServiceTitanBusinessAttribution'],
) as dag:
        dag.doc_md = __doc__

        # airflow dag task 1 to pull calls data
        PullDataFromCallsApi = PythonOperator(
            task_id='extract_service_titan_business',
            python_callable=extract_service_titan_business,
            on_failure_callback=putil.error_callback_sns,
            dag=dag,
            provide_context=True  )

        PullDataFromSrcCollection = PythonOperator(
            task_id='enhance_records',
            python_callable=get_and_enhance_records,
            on_failure_callback=putil.error_callback_sns,
            dag=dag,
            provide_context=True 
        )
        # PullProjectData=PythonOperator(
        #     task_id='project_task',
        #     python_callable=get_project_data,
        #     dag=dag,
        #     provide_context=True 
        # )
        mergeProjectDetailsTask = PythonOperator(
            task_id='merge_project_details_task',
            python_callable=merge_project_details,
            on_failure_callback=putil.error_callback_sns,
            provide_context=True,
            dag=dag
        )
        trigger_dependent_dag= TriggerDagRunOperator(
            task_id="trigger_dependent_dag",
            trigger_dag_id="ServiceTitanBusinessAttributionTransform",
            conf={"conf":"{{ dag_run.conf }}"},
            on_failure_callback=putil.error_callback_sns,
            dag=dag,
            wait_for_completion=False
        )
        # taks to skip the other tasks when there is no data or ingest log exists
        # Define ShortCircuitOperator for conditional execution of downstream tasks
        skip_downstream_tasks = ShortCircuitOperator(
            task_id='skip_downstream_tasks',
            python_callable=decide_to_skip,
            provide_context=True,
            dag=dag
        )

        # to handel err cases
        error_task = PythonOperator(
            task_id='error',
            python_callable=error,
            dag=dag,
            trigger_rule = "one_failed"
        )

        # PullDataFromCallsApi >> skip_downstream_tasks >> [PullDataFromSrcCollection,PullProjectData, mergeProjectDetailsTask, trigger_dependent_dag] >> error_task


        # Define task dependencies
        PullDataFromCallsApi >> skip_downstream_tasks

        # Branching for conditional execution
        skip_downstream_tasks >> PullDataFromSrcCollection

        PullDataFromSrcCollection >> mergeProjectDetailsTask
        # # PullProjectData >> mergeProjectDetailsTask
        mergeProjectDetailsTask >> trigger_dependent_dag

        # Define the error task to run only if any of the tasks fail
        [PullDataFromSrcCollection,  trigger_dependent_dag]>> error_task

        # PullProjectData >> mergeProjectDetailsTask