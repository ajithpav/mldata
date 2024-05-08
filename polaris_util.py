import ast

from bson.json_util import dumps
from bson.objectid import ObjectId
import pymongo
import pytz
import urllib
import json
import re
from airflow.models import Variable
import logging
from datetime import datetime, timedelta, timezone,date
import pytz
from dateutil.relativedelta import relativedelta
import hashlib
import pandas as pd

TODAYS_DATE = date.today()

global MONGO_URL
global MONGO_DBNAME  
global SYS_DATA_SOURCE_COLLECTION
global TENANT_DATA_SOURCE_MAPPING_COLLECTION
global TENANT_INGEST_LOG_COLLECTION 
global RAW_COLLECTION 
global SQL_HOSTNAME     
global SQL_USERNAME 
global SQL_PASSWORD 
global SQL_DBNAME     

MONGO_DBNAME =  Variable.get("MONGO_DBNAME")
MONGO_USERNAME = Variable.get("POLARIS_DB_USERNAME")
MONGO_PWD = Variable.get("POLARIS_DB_PWD")
MONGO_HOST = Variable.get("POLARIS_DB_HOST")


SYS_DATA_SOURCE_COLLECTION =  Variable.get("SYS_DATA_SOURCE_COLLECTION")
TENANT_DATA_SOURCE_MAPPING_COLLECTION =  Variable.get("TENANT_DATA_SOURCE_CONFIGURATION_COLLECTION")
TENANT_INGEST_LOG_COLLECTION =  Variable.get("TENANT_INGEST_LOG_COLLECTION")
MONGODB_SERVER_SELECTION_TIMEOUT_MS = Variable.get("MONGODB_SERVER_SELECTION_TIMEOUT_MS")
TENANTS_COLLECTION = "Tenants"

MONGO_URL = "mongodb+srv://" + MONGO_USERNAME +":" + urllib.parse.quote(MONGO_PWD)+"@" + MONGO_HOST+ "/?retryWrites=true&w=majority&ssl=true&serverSelectionTimeoutMS="+str(MONGODB_SERVER_SELECTION_TIMEOUT_MS)
TenantBcServicesMapping = "TenantBcServicesMapping"
SysBcServicesCollection = "SysBcServices"  
SysProductsCollection = "SysProducts"

SQL_HOSTNAME = Variable.get("RRP_DB_HOSTNAME")
#"rrp-dev.cpih0agyxjcf.us-east-1.rds.amazonaws.com"
## Replace individual credentials with system credentials.. 
SQL_USERNAME = Variable.get("RRP_DB_USERNAME")
# "girish" #"suja"
SQL_PASSWORD =  Variable.get("RRP_DB_PWD")
# "6uWjFAK7fx" #"5FUBwlS1Ed"
SQL_DBNAME =  Variable.get("RRP_DB_NAME")
# "seodocsdb"
    
## DataBlueCorona global variables
BLUE_CORONA_MYSQL_HOSTNAME="devdb.cpih0agyxjcf.us-east-1.rds.amazonaws.com"
BLUE_CORONA_MYSQL_USER="tyost"
BLUE_CORONA_MYSQL_PASSWORD="g4TsVB0h75QKqy3RV7"
BLUE_CORONA_MYSQL_DB="seodocsdb"    


## Auto trigger historical ingestion variables
DEFAULT_HISTORICAL_LIMIT_IN_MONTHS = Variable.get("DEFAULT_HISTORICAL_LIMIT_IN_MONTHS")
DEFAULT_DATA_START_DATE = Variable.get("DEFAULT_DATA_START_DATE")
DEFAULT_HISTORICAL_BATCH_SIZE = Variable.get("DEFAULT_HISTORICAL_BATCH_SIZE")
DEFAULT_HISTORICAL_BATCH_UNIT = Variable.get("DEFAULT_HISTORICAL_BATCH_UNIT")

WORKFLOW_TRIGGER_SEQUENCE = Variable.get("WORKFLOW_TRIGGER_SEQUENCE")

#########################################################################################################################
def sentry_error_finder():
    if Variable.get('POLARIS_ENVIRONMENT') != "LOCAL":
        import sentry_sdk
        sentry_sdk.init(
            dsn=Variable.get("SENTRY_DNS"), #"https://3a2f30183da9abcf4ab723de5975eec4@o4507031306371072.ingest.us.sentry.io/4507141134417920"
            environment=Variable.get("POLARIS_ENVIRONMENT"),
            traces_sample_rate=1.0,
            # debug=True,
            profiles_sample_rate=1.0,
            # enable_tracing=True
        )
    return None
#########################################################################################################################
def get_database():
    client = pymongo.MongoClient(MONGO_URL)    
    polarisdb = client[MONGO_DBNAME]
    return polarisdb
#########################################################################################################################
def get_sys_datasource_details(dataSourceName, dag_name):
    logging.info( dag_name +" : Getting System data source detail")
    client = pymongo.MongoClient(MONGO_URL)    
    polarisdb = client[MONGO_DBNAME]
    sysDataSourceCfg= polarisdb[SYS_DATA_SOURCE_COLLECTION]
    dataSource = sysDataSourceCfg.find({ 'dataSourceName': dataSourceName})
    return dataSource      
#########################################################################################################################
def get_sys_datasource_extractWorkFlow(dependentDataSource):
    extractWorkFlow = ""
    logging.info("Getting System data source Extract")
    client = pymongo.MongoClient(MONGO_URL)    
    polarisdb = client[MONGO_DBNAME]
    sysDataSourceCfg= polarisdb[SYS_DATA_SOURCE_COLLECTION]
    dataSource = json.loads(dumps(sysDataSourceCfg.find({"_id":str(dependentDataSource)})))
    if dataSource is not None:
        extractWorkFlow = dataSource[0]['ingestionWorkflows']['dataExtraction']['workflow']
    return extractWorkFlow
################################################################################################################################
def get_tenant_datasource_details(platformName, tenantId, dag_name):
    logging.info( dag_name +" : Getting tenant datasource with account details for "+platformName)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantDataSourceCfg= polarisdb[TENANT_DATA_SOURCE_MAPPING_COLLECTION]
    dataSourceCfg = tenantDataSourceCfg.find({ 'platform': platformName, 'tenantId' : tenantId})
    print("dataSourceCfg",dataSourceCfg)
    return dataSourceCfg  
#########################################################################################################################
def get_tenant_locations(tenantId):
    logging.info("Getting tenant locations")
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]    
    sysChannel = polarisdb["TenantLocations"]
    # tenantlocations = sysChannel.find({"tenantId": ObjectId(tenantId)})
    tenantlocations = sysChannel.find({"tenantId": ObjectId(tenantId), "status": True})
    tenantlocations = json.loads(dumps(tenantlocations))
    location_active = [location.get('locationName') for location in tenantlocations]
    #for convenince this is being added
    for location in location_active:
        print("Location  where status is Active:",location)
    return tenantlocations
#########################################################################################################################
def get_tenant_location_groups(tenantId):
    logging.info("Getting tenant locations groups")
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sysChannel = polarisdb["TenantLocationGroups"]
    teantlocationGroups = sysChannel.find({"tenantId": tenantId})
    teantlocationGroups = json.loads(dumps(teantlocationGroups))
    return teantlocationGroups
#########################################################################################################################
def get_tenant_market_segments(tenantId):
    logging.info("Getting tenant locations market segments")
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sysChannel = polarisdb["TenantMarketSegments"]
    teantmarketSegments = sysChannel.find({"tenantId": tenantId})
    teantmarketSegments = json.loads(dumps(teantmarketSegments))
    return teantmarketSegments
#########################################################################################################################
def get_tenant_campaign_groups(tenantId):
    
    # client = pymongo.MongoClient(MONGO_URL)
    # polarisdb = client[MONGO_DBNAME]
    # sysChannel = polarisdb["TenantCampaignGroups"]
    # campaignGroups = sysChannel.find({'tenantId' : tenantId})
    # campaignGroups = json.loads(dumps(campaignGroups))
    # return campaignGroups

    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    TenantcampaignGroups = polarisdb["TenantCampaignGroups"]
    # Fetch the entire document based on the tenantId
    query = {'tenantId': tenantId}
    campaignGroups = TenantcampaignGroups.find_one(query)


    #getting the default campaign group mapping rules
    if campaignGroups is None:
        # Return an empty array
        print("There are no tenant level Campaign groups")
        return {'mappingRules': []}

    # Filter out only the active mapping rules
    active_mapping_rules = [rule for rule in campaignGroups['mappingRules'] if not rule.get('deleteStatus', False)]
    mapping_rules_false_delete_status = [rule for rule in campaignGroups.get('mappingRules', []) if rule.get('deleteStatus', True)]
    #for convenince this is being added
    for false_map_rule in mapping_rules_false_delete_status:
        print("Skipped campaign since its deleted status is Active:",false_map_rule.get('campaignGroup'))

    # Update the document with the filtered mapping rules
    campaignGroups['mappingRules'] = active_mapping_rules
    return campaignGroups
######################################################################################################################### 
def get_sys_campaign_groups():
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
          
    sysCampaignGroups_collection = polarisdb["SysCampaignGroups"]
    return sysCampaignGroups_collection

#########################################################################################################################
def get_campaignGroupId(regex_match_lst,campaignGroups, campaignGroupRegex):
    dicts = {}
    try:
        regex_match_lst = regex_match_lst if isinstance(regex_match_lst, list) and regex_match_lst else [regex_match_lst]
        dicts['campaignGroupId'] =  ""
        df = pd.DataFrame(campaignGroups)
        if len(campaignGroupRegex) != 0 and campaignGroups:
            for i in regex_match_lst:
                print("campaign : ", i)
                if i not in  ['', ' ']:
                    df1 = pd.DataFrame(campaignGroups['mappingRules']).to_dict('records')
                    if len(df1):
                        for regex in df1 :
                                filtered_df = pd.Series(i).str.contains(regex['campaignNameRegex'], regex=True, case=False)
                                if filtered_df[0]:
                                    dicts['campaignGroupId'] = regex['campaignGroupId']
                                    return dicts
                                else:
                                    dicts['campaignGroupId'] = '_other'
                                    if 'landingPagePathRegex' in regex and regex['landingPagePathRegex'] != '':
                                        filtered_df = pd.Series(i).str.contains(regex['landingPagePathRegex'], regex=True,
                                                                                case=False)
                                        if filtered_df[0]:
                                            dicts['campaignGroupId'] =  regex['campaignGroupId']
                                            return dicts
                                        else:
                                            dicts['campaignGroupId'] = '_other'
                                    else :
                                        dicts['campaignGroupId'] = '_other'
                    else:
                        dicts['campaignGroupId'] = '_other'
            return dicts
        else:
            dicts['campaignGroupId'] = '_other'
        print("campaignGroupId : ", dicts['campaignGroupId'])

        if len(campaignGroupRegex) != 0 and dicts['campaignGroupId'] == "":
            dicts['campaignGroupId'] = '_other'
        return dicts
    except:
        dicts['campaignGroupId'] = '_other'
        return dicts
#########################################################################################################################  

def error_callback_sns(context):
    from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
    from bson.objectid import ObjectId
    try:
        data_params = ast.literal_eval(context['params']['conf']) if 'conf' in context['params'] else context['params']
        dag_name =data_params.get('dag_name')
        task_instance = context['task_instance']
        log_url = task_instance.log_url
        task_status = task_instance.current_state()
        execution_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        tenantId = data_params['tenant']
        client = pymongo.MongoClient(MONGO_URL)
        polarisdb = client[MONGO_DBNAME]
        Tenants = polarisdb["Tenants"]
        Tenants = list(Tenants.find({'_id': ObjectId(tenantId)}))
        # source_name = dag_name if dag_name else data_params.get('dataSourceName')
        # source_name_mail = source_name if source_name else data_params.get('data_source_name')
        message = f"{context['dag']}DAG Failure Alert View logs for {Tenants[0]['name'] if Tenants else data_params.get('tenant')}: {log_url}' - ,Dag name: {context['dag']},  execution_date: {execution_date}, Task status: Failed "        
        print("message : ", message)
        op = SnsPublishOperator(
            task_id='failure',
            aws_conn_id="SNS",
            target_arn=Variable.get('TARGET-ARN'),  # dev sns topic ARN
            message=message,
            subject="Test DAG Failed",
        )
        op.execute(context)
    except Exception as er:
        context['ti'].log.error(f"Error occurred: {er}")

#########################################################################################################################
def get_tenant_campaigns(tenantId, campaign, campaignGroupId):
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
          
    tenantCampaign = polarisdb["TenantCampaigns"]
    tenantCampaigns = tenantCampaign.find({'tenantId' : tenantId, 'name':campaign, 'campaignGroupId':campaignGroupId})
    tenantCampaigns = json.loads(dumps(tenantCampaigns))
    return tenantCampaigns

######################################################################################################################### 

def insert_tenantCampaigns_data(tenantId, campaign, campaignGroupId):
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
          
    tenantCampaign = polarisdb["TenantCampaigns"]
    dict = {'tenantId' : tenantId, 'name':campaign, 'campaignGroupId':campaignGroupId}
    tenantCampaign.insert_one(dict)
    recordsCount = tenantCampaign.count_documents(dict)
    logging.info("Count of inserted records from tenantCampaign is "+str(recordsCount))    
    
#########################################################################################################################   

def update_sysCampaignGroups(dict):
    sysCampaignGroups_collection = get_sys_campaign_groups()
    sysCampaignGroups = sysCampaignGroups_collection.find_one({'_id': dict['entity']['campaignGroupId']})
    
    if sysCampaignGroups != None:
        if len(sysCampaignGroups) > 0:    
            dict['entity']['campaignGroup'] = sysCampaignGroups['name']
            channel_flag = 0
            for channel in sysCampaignGroups['channels']:
                if dict['entity']['channelNameId'] == channel['channelNameId']:
                    channel_flag = 1
                    break            
            if channel_flag == 0:                
                channel_lst = sysCampaignGroups['channels']
                channel_lst.append({"channelNameId": dict['entity']['channelNameId'], "channelId": dict['entity']['channelId'], "platformId": dict['entity']['platformId'], "campaignTypeId": dict['entity']['campaignTypeId']})                               
                logging.info("Updating sysCampaignGroups ")
                match_dict = {'_id' : sysCampaignGroups['_id'], 'name':sysCampaignGroups['name']}
                updated_sysCampaignGroups = sysCampaignGroups_collection.find_one_and_update(match_dict,{"$set":{'channels':channel_lst}},upsert=True)
    else:
        channel_data = []
        channel_data.append({"channelNameId": dict['entity']['channelNameId'], "channelId": dict['entity']['channelId'],
                            "platformId": dict['entity']['platformId'],
                            "campaignTypeId": dict['entity']['campaignTypeId']})
        try:
            campaignGroupId_data = dict['entity']['campaignGroupId'].split('_')[-1] if dict['entity']['campaignGroupId'] else ""
        except:
            campaignGroupId_data = ""   
        updated_sysCampaignGroups = sysCampaignGroups_collection.insert_one({'_id':dict['entity']['campaignGroupId'],
                                                                             'name': campaignGroupId_data,
                                                                             'channels': channel_data})
        dict['entity']['campaignGroup'] = campaignGroupId_data

    
    return dict 
#########################################################################################################################
    
def set_campaignId(dict):    
    tenantCampaigns = get_tenant_campaigns(dict['entity']['tenantId'], dict['entity']['campaign'], dict['entity']['campaignGroupId'])    
    if tenantCampaigns != None:
        if len(tenantCampaigns) == 0 and dict['entity']['campaignGroupId'] != "":
            insert_tenantCampaigns_data(dict['entity']['tenantId'], dict['entity']['campaign'], dict['entity']['campaignGroupId'])
    
    tenantCampaigns = get_tenant_campaigns(dict['entity']['tenantId'], dict['entity']['campaign'], dict['entity']['campaignGroupId'])
    if tenantCampaigns != None:
        if len(tenantCampaigns) != 0 and dict['entity']['campaignGroupId'] != "":
            dict['entity']['campaignId'] = tenantCampaigns[0]['_id']['$oid']
    
    return dict    
    
#########################################################################################################################
def getChannelMapping():
    logging.info("Getting channels")
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]    
    sysChannel = polarisdb["SysChannels"]
    channelMappings = sysChannel.find({"type": "channel_name"})
    channelMappings = json.loads(dumps(channelMappings))
    return channelMappings    
#########################################################################################################################
# def getChannelPlatformCampaignName(Id):
#     print("channel get the channel name for the id",Id)
#     client = pymongo.MongoClient(MONGO_URL)
#     polarisdb = client[MONGO_DBNAME]    
#     sysChannel = polarisdb["SysChannels"]
#     channelMappings = sysChannel.find({'_id': Id})
#     channelMappings = json.loads(dumps(channelMappings))
#     name = None
#     for items in channelMappings:
#         name = items['displayName']
#     if name:
#         return name 

def getChannelPlatformCampaignName(Id):
    print("channel get the channel name for the id",Id)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]    
    sysChannel = polarisdb["SysChannels"]
    channelMappings = sysChannel.find({'_id': Id})
    channelMappings = json.loads(dumps(channelMappings))
    name = None
    for items in channelMappings:
        name = items['displayName']
    if name:
        return name 
#########################################################################################################################
def get_location_id(regex_match_lst,tenantlocations, locationRegex):
    
    dict = {}
    dict['locationId'] =  None
    dict['locationName'] = ""
    dict['locationGroupId'] =  None
    dict['marketSegmentId'] =  None
    if len(locationRegex) != 0:
        
        for location in tenantlocations:    
            for matchRule in location['mappingRules']:
                for locationReg, regex_match in zip(locationRegex, regex_match_lst):
                    if locationReg in matchRule:
                        if re.search(matchRule[locationReg], regex_match,re.IGNORECASE) and matchRule[locationReg] != "":
                            dict['locationId'] = location['_id']
                            dict['locationName'] = location['locationName']
                            if "locationGroup" in location:
                                dict['locationGroupId'] = location['locationGroup']
                            if "marketSegment" in location:
                                dict['marketSegmentId'] = location['marketSegment']                            
                            return dict                            
    return dict
#########################################################################################################################
def get_location_group_id(regex_match_lst,tenantlocationGroups, locationRegex):
    
    dict = {}
    dict['locationGroupId'] =  None
    dict['marketSegmentId'] =  None
    if len(locationRegex) != 0:
        
        for locationGroup in tenantlocationGroups:            
            for matchRule in locationGroup['mappingRules']:
                for locationReg, regex_match in zip(locationRegex, regex_match_lst):  
                    if locationReg in matchRule:
                        if re.search(matchRule[locationReg], regex_match,re.IGNORECASE) and matchRule[locationReg] != "":
                            dict['locationGroupId'] = locationGroup['_id']
                            if "marketSegment" in locationGroup:
                                dict['marketSegmentId'] = locationGroup['marketSegment']                                
                            return dict                            
    return dict
#########################################################################################################################
def get_market_segment_id(regex_match_lst,tenantmarketSegments, locationRegex):
    
    dict = {}
    dict['marketSegmentId'] =  None
    if len(locationRegex) != 0:
        
        for marketSegment in tenantmarketSegments:      
            for matchRule in marketSegment['mappingRules']:
                for locationReg, regex_match in zip(locationRegex, regex_match_lst):  
                    if locationReg in matchRule:
                        if re.search(matchRule[locationReg], regex_match,re.IGNORECASE) and matchRule[locationReg] != "":
                            dict['marketSegmentId'] = marketSegment['_id']    
                            return dict                         
    return dict
#########################################################################################################################
def get_channel_id(source,medium,campaign,channelMappings):

    print("channelMappings",channelMappings)
    dict = {}
    dict['channelNameId'] =  ''
    dict['channelId'] = ''
    dict['platformId'] =  ''
    dict['campaignTypeId'] = ''
    dict['channelTypeName'] = ''
    dict['channelName'] = ''
    dict['platformName'] = ''
    dict['campaignTypeName'] = ''
   
    if channelMappings != None and len(channelMappings) != 0:
        for channelRule in channelMappings:
            sourceMatch = False
            mediumMatch = False
            campaignMatch = False
            excludeSourceMatch = False
            excludeMediumMatch = False
            excludeCampaignMatch = False  
            if "mappingRules" in channelRule:
            
                for rule in channelRule["mappingRules"]:
                    if 'source' in rule:
                        if rule['source']  is None or re.search(rule['source'] ,source,re.IGNORECASE):
                            sourceMatch = True 
                    if 'medium' in rule:
                        if rule['medium'] is None or re.search(rule['medium'],medium,re.IGNORECASE):
                            mediumMatch = True
                    if 'campaign' in rule:
                        if rule['campaign'] is None or re.search(rule['campaign'],campaign,re.IGNORECASE):
                            campaignMatch = True
                    if 'excludeSource' in rule:
                        if rule['excludeSource'] is not None and re.search(rule['excludeSource'],source,re.IGNORECASE):
                            excludeSourceMatch = True        
                    if 'excludeMedium' in rule:
                        if rule['excludeMedium'] is not None and  re.search(rule['excludeMedium'],medium,re.IGNORECASE):
                            excludeMediumMatch = True
                    if 'excludeCampaign' in rule:
                        if rule['excludeCampaign'] is not None and  re.search(rule['excludeCampaign'],campaign,re.IGNORECASE):
                            excludeCampaignMatch = True                                                    
            if sourceMatch and mediumMatch and campaignMatch and not excludeSourceMatch and not excludeMediumMatch and not excludeCampaignMatch:            
                dict['channelNameId'] =  channelRule['_id']
                dict['channelId'] = channelRule['channel']
                dict['channelName'] = getChannelPlatformCampaignName(dict['channelId'])
                dict['platformId'] =  channelRule['platform']
                dict['platformName'] = getChannelPlatformCampaignName(dict['platformId'])
                dict['campaignTypeId'] = channelRule['campaignType']
                dict['campaignTypeName'] = getChannelPlatformCampaignName(dict['campaignTypeId'])
                dict['channelTypeName'] = channelRule['displayName']
        
    if (dict['channelNameId'] == '') and (dict['channelId'] == '') and (dict['platformId'] == '') and (dict['campaignTypeId'] == '') and (dict['channelTypeName'] == ''):
       

        client = pymongo.MongoClient(MONGO_URL)
        polarisdb = client[MONGO_DBNAME]
        sysChannel = polarisdb["SysChannels"]

        # MongoDB query to fetch the record from 'SysChannels' collection which does not have 'mappingRules' and 'type' is 'channel_name'.
        query = {
            "$and": [
                {"mappingRules": {"$exists": False}},
                {"type": "channel_name"}
            ]
        }

        channelMappings = sysChannel.find(query)
        channelMappings = list(channelMappings)
        
        
        dict={}
        dict['channelNameId'] =  None
        dict['channelId'] = None
        dict['platformId'] =  None
        dict['campaignTypeId'] = None
        dict['channelTypeName'] = None
        dict['channelName'] = None
        dict['platformName'] = None
        dict['campaignTypeName'] = None
        if channelMappings:
                dict1=channelMappings[0]
                dict['channelNameId'] =  dict1.get('_id')
                dict['channelId'] = dict1.get('channel')        
                dict['channelName'] = dict1.get('displayName')
                dict['platformId'] =  dict1.get('platform')  
                dict['platformName'] = dict1.get('displayName')
                dict['campaignTypeId'] = dict1.get('campaignType')
                dict['campaignTypeName'] = dict1.get('displayName')
                dict['channelTypeName'] = dict1.get('displayName')
                

    return dict

#########################################################################################################################
def get_location_ids_info(dict, regex_match_lst,tenant_locations,tenant_locationGroups,
                            tenantmarketSegments,locationRegex):
    logging.info("Getting location ids info")
    
    locations_dict = get_location_id(regex_match_lst, tenant_locations, locationRegex)
    if locations_dict['locationId'] == None:
        dict['entity']['locationId'] = None
        dict['entity']['locationGroupId'] = None
        dict['entity']['marketSegmentId'] = None
    elif '$oid' in locations_dict['locationId']:
        dict['entity']['locationId'] = locations_dict['locationId']['$oid']
        if locations_dict['locationGroupId'] == None:
            dict['entity']['locationGroupId'] = None
        elif '$oid' in locations_dict['locationGroupId']:
            dict['entity']['locationGroupId'] = locations_dict['locationGroupId']['$oid']
        else:
            dict['entity']['locationGroupId'] = locations_dict['locationGroupId']
        if locations_dict['marketSegmentId'] == None:
            dict['entity']['marketSegmentId'] = None
        elif '$oid' in locations_dict['marketSegmentId']:
            dict['entity']['marketSegmentId'] = locations_dict['marketSegmentId']['$oid']
        else:
            dict['entity']['marketSegmentId'] = locations_dict['marketSegmentId']    
    elif locations_dict['locationId'] != None:
        dict['entity']['locationId'] = locations_dict['locationId']
        if locations_dict['locationGroupId'] == None:
            dict['entity']['locationGroupId'] = None
        elif '$oid' in locations_dict['locationGroupId']:
            dict['entity']['locationGroupId'] = locations_dict['locationGroupId']['$oid']
        else:
            dict['entity']['locationGroupId'] = locations_dict['locationGroupId']
        if locations_dict['marketSegmentId'] == None:
            dict['entity']['marketSegmentId'] = None
        elif '$oid' in locations_dict['marketSegmentId']:
            dict['entity']['marketSegmentId'] = locations_dict['marketSegmentId']['$oid']
        else:
            dict['entity']['marketSegmentId'] = locations_dict['marketSegmentId'] 
    
    if locations_dict['locationId'] == None or locations_dict['locationId'] == "" or locations_dict['locationGroupId'] == None:
        locationGrps_dict = get_location_group_id(regex_match_lst, tenant_locationGroups, locationRegex)
        if locationGrps_dict['locationGroupId'] == None:
            dict['entity']['locationGroupId'] = None
            dict['entity']['marketSegmentId'] = None
        elif '$oid' in locationGrps_dict['locationGroupId']:
            dict['entity']['locationGroupId'] = locationGrps_dict['locationGroupId']['$oid']
            if locationGrps_dict['marketSegmentId'] == None:
                dict['entity']['marketSegmentId'] = None
            elif '$oid' in locationGrps_dict['marketSegmentId']:
                dict['entity']['marketSegmentId'] = locationGrps_dict['marketSegmentId']['$oid']
            else:
                dict['entity']['marketSegmentId'] = locationGrps_dict['marketSegmentId'] 
        elif locationGrps_dict['locationGroupId'] != None:
            dict['entity']['locationGroupId'] = locationGrps_dict['locationGroupId']
            if locationGrps_dict['marketSegmentId'] == None:
                dict['entity']['marketSegmentId'] = None
            elif '$oid' in locationGrps_dict['marketSegmentId']:
                dict['entity']['marketSegmentId'] = locationGrps_dict['marketSegmentId']['$oid']
            else:
                dict['entity']['marketSegmentId'] = locationGrps_dict['marketSegmentId'] 
    
        if locationGrps_dict['locationGroupId'] == None or locationGrps_dict['locationGroupId'] == "" or locationGrps_dict['marketSegmentId'] == None:
            market_seg_dict = get_market_segment_id(regex_match_lst, tenantmarketSegments, locationRegex)
            if market_seg_dict['marketSegmentId'] == None:
                dict['entity']['marketSegmentId'] = None
            elif '$oid' in market_seg_dict['marketSegmentId']:
                dict['entity']['marketSegmentId'] = market_seg_dict['marketSegmentId']['$oid']
            elif market_seg_dict['marketSegmentId'] != None:
                dict['entity']['marketSegmentId'] = market_seg_dict['marketSegmentId']

    ## timezone
    if dict['entity']['locationId'] != None and dict['entity']['locationId'] != "":
        location = get_location_by_locationId(dict['entity']['locationId'])
        tmzone = get_location_timezone(location, dict['entity']['tenantId'])
        dict = datetime_utc_conversion(dict,tmzone)
    elif dict['entity']['locationGroupId'] != None and dict['entity']['locationGroupId'] != "":
        location = get_location_by_locationGroup(dict['entity']['locationGroupId'])
        tmzone = get_location_timezone(location, dict['entity']['tenantId'])
        dict = datetime_utc_conversion(dict,tmzone)
    elif dict['entity']['marketSegmentId'] != None and dict['entity']['marketSegmentId'] != "":
        location = get_location_by_marketSegment(dict['entity']['marketSegmentId'])
        tmzone = get_location_timezone(location, dict['entity']['tenantId'])
        dict = datetime_utc_conversion(dict,tmzone) 
        
    ## dayOfWeek and hourOfDay
    dict['entity']['dayOfWeek'] = None
    #dict['entity']['hourOfDay'] = None
    if isinstance(dict['date'], datetime):
        dict['entity']['dayOfWeek'] = dict['entity']['date'].isoweekday()
        #dict['entity']['hourOfDay'] = dict['entity']['date'].hour
        
    return dict
#########################################################################################################################

def get_location_timezone(location, tenantId):
    tmzone = {}
    if len(location) > 0:
        if 'timeZone' in location[0]:
            tmzone = location[0]['timeZone']
        else:
            logging.info("No timezone found at location level")
            tmzone = get_client_timezone(tenantId)        
        
        if tmzone == None:
            tmzone = {}
            logging.info("Null timezone found at location and tenant level")
        elif len(tmzone) == 0:
            logging.info("No timezone found at location and tenant level")
    
    return tmzone

#########################################################################################################################
def get_client_timezone(tenantId):
    tmzone = {}
    tenant = get_tenant(tenantId)
    if 'timeZone' in tenant[0]:
        tmzone = tenant[0]['timeZone']
    if tmzone == None:
        tmzone = {}
        logging.info("Null timezone found at tenant level")
    elif len(tmzone) == 0:
        logging.info("No timezone found at tenant level")
        
    return tmzone
    
#########################################################################################################################

def datetime_utc_conversion(dict,tmzone):
    if isinstance(dict['date'], datetime) and len(tmzone) != 0:
        if ("id" in tmzone):
            input_timezone = pytz.timezone(tmzone['id'])
            local_date = input_timezone.localize(dict['date'])
            dict['date'] = local_date.astimezone(pytz.utc)
    if isinstance(dict['entity']['date'], datetime) and len(tmzone) != 0:
        if ("id" in tmzone):
            input_timezone = pytz.timezone(tmzone['id'])
            local_date = input_timezone.localize(dict['entity']['date'])
            dict['entity']['date'] = local_date.astimezone(pytz.utc)        
        
    return dict   
#########################################################################################################################
def get_location_Names_info(dict, regex_match_lst,tenant_locations,
                            locationRegex):
    
    locations_dict = get_location_id(regex_match_lst, tenant_locations, locationRegex)   
    if locations_dict['locationId'] == None:
        dict['entity']['locationName'] = ""
    elif '$oid' in locations_dict['locationId']:
        dict['dimensions']['locationName'] = locations_dict['locationName']
    elif locations_dict['locationId'] != None:
        dict['dimensions']['locationName'] = locations_dict['locationName']    
    return dict
#########################################################################################################################
def insert_to_mongo(collection, tenantId, data):
    logging.info("Inserting into mongo collection : "+collection)
    print("count of records passed to insert from the dag",len(data))
    print("data from putil",data)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    collecion_to_insert = polarisdb[collection]
    result = collecion_to_insert.insert_many(data)
    if result is not None:
        inserted_ids = result.inserted_ids
        logging.info(f"Successfully inserted {len(inserted_ids)} records")
    else:
        logging.error("Failed to insert data for gbp!!")
#########################################################################################################################
def get_general_locations(dict, tenant_locations, tenant_locationGroups,tenantmarketSegments):
    if (dict['entity']['locationId'] == None or dict['entity']['locationId'] == "") and (dict['entity']['locationGroupId'] == None or dict['entity']['locationGroupId'] == "") and (dict['entity']['marketSegmentId'] == None or dict['entity']['marketSegmentId'] == ""):
        ## timezone
        tmzone = get_client_timezone(dict['entity']['tenantId'])
        dict = datetime_utc_conversion(dict,tmzone)                    
        
        ## dayOfWeek and hourOfDay
        dict['entity']['dayOfWeek'] = None
        #dict['entity']['hourOfDay'] = None
        if isinstance(dict['date'], datetime):
            dict['entity']['dayOfWeek'] = dict['entity']['date'].isoweekday()
            #dict['entity']['hourOfDay'] = dict['entity']['date'].hour
            
    if (dict['entity']['locationId'] == None or dict['entity']['locationId'] == ""):
        for locationId in tenant_locations:
            if locationId['locationName'] == 'General':
                if '$oid' in locationId['_id']:
                    dict['entity']['locationId'] = locationId['_id']['$oid']
                elif locationId['_id'] != None:
                    dict['entity']['locationId'] = locationId['_id'] 
                    
    if (dict['entity']['locationGroupId'] == None or dict['entity']['locationGroupId'] == ""):
        for locationGroup in tenant_locationGroups:
            if locationGroup['name'] == 'General':
                if '$oid' in locationGroup['_id']:
                    dict['entity']['locationGroupId'] = locationGroup['_id']['$oid']
                elif locationGroup['_id'] != None:
                    dict['entity']['locationGroupId'] = locationGroup['_id']                
    
    if (dict['entity']['marketSegmentId'] == None or dict['entity']['marketSegmentId'] == ""):    
        for marketSegment in tenantmarketSegments:
            if marketSegment['name'] == 'General':
                if '$oid' in marketSegment['_id']:
                    dict['entity']['marketSegmentId'] = marketSegment['_id']['$oid']
                elif marketSegment['marketSegmentId'] != None:
                    dict['entity']['marketSegmentId'] = marketSegment['_id']           

    
    return dict
# using row:
def get_general_locations_for_pandas(row, tenant_locations, tenant_locationGroups,tenantmarketSegments):
    if (row['entity']['locationId'] == None or row['entity']['locationId'] == "") and (row['entity']['locationGroupId'] == None or row['entity']['locationGroupId'] == "") and (row['entity']['marketSegmentId'] == None or row['entity']['marketSegmentId'] == ""):
        ## timezone
        tmzone = get_client_timezone(row['entity']['tenantId'])
        row = datetime_utc_conversion(row,tmzone)                    
        
        ## dayOfWeek and hourOfDay
        row['entity']['dayOfWeek'] = None
        #dict['entity']['hourOfDay'] = None
        if isinstance(row['date'], datetime):
            row['entity']['dayOfWeek'] = row['entity']['date'].isoweekday()
            #dict['entity']['hourOfDay'] = dict['entity']['date'].hour
            
    if (row['entity']['locationId'] == None or row['entity']['locationId'] == ""):
        for locationId in tenant_locations:
            if locationId['locationName'] == 'General':
                if '$oid' in locationId['_id']:
                    row['entity']['locationId'] = locationId['_id']['$oid']
                elif locationId['_id'] != None:
                    row['entity']['locationId'] = locationId['_id'] 
                    
    if (row['entity']['locationGroupId'] == None or row['entity']['locationGroupId'] == ""):
        for locationGroup in tenant_locationGroups:
            if locationGroup['name'] == 'General':
                if '$oid' in locationGroup['_id']:
                    row['entity']['locationGroupId'] = locationGroup['_id']['$oid']
                elif locationGroup['_id'] != None:
                    row['entity']['locationGroupId'] = locationGroup['_id']                
    
    if (row['entity']['marketSegmentId'] == None or row['entity']['marketSegmentId'] == ""):    
        for marketSegment in tenantmarketSegments:
            if marketSegment['name'] == 'General':
                if '$oid' in marketSegment['_id']:
                    row['entity']['marketSegmentId'] = marketSegment['_id']['$oid']
                elif marketSegment['marketSegmentId'] != None:
                    row['entity']['marketSegmentId'] = marketSegment['_id']           

    
    return row

#########################################################################################################################  
def get_tenant(tenantId):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenant= polarisdb["Tenants"]
    tenant = tenant.find({ '_id': ObjectId(tenantId)})
    tenant = json.loads(dumps(tenant))
    return tenant
######################################################################################################################### 
def get_client_region(tenantId):
    logging.info("Getting client region for tenantId "+tenantId)
    dict = {}
    dict['clientRegion'] = ""    
    tenant = get_tenant(tenantId)
    if tenant: 
        if "clientRegion" in tenant[0]:
            dict['clientRegion'] = tenant[0]['clientRegion']            
        else:
            dict['clientRegion'] = ""
    return dict      
#########################################################################################################################
def get_sysRegion_by_regionAbbr(stateAbbr):
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sysRegion= polarisdb["SysRegions"]
    sysRegion = sysRegion.find({"regionAbbr": stateAbbr})
    sysRegion = json.loads(dumps(sysRegion))
    sysRegion = json.loads(dumps(sysRegion))
    return sysRegion
#########################################################################################################################
def get_sysRegion_by_region(state):
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sysRegion= polarisdb["SysRegions"]
    sysRegion = sysRegion.find({"region": state})
    sysRegion = json.loads(dumps(sysRegion))
    sysRegion = json.loads(dumps(sysRegion))
    return sysRegion
#########################################################################################################################
def get_location_by_locationId(locationId):
    client = pymongo.MongoClient(MONGO_URL)
    
    polarisdb = client[MONGO_DBNAME]
    location= polarisdb["TenantLocations"]
    location = location.find({ '_id': ObjectId(locationId)})
    location = json.loads(dumps(location))    
    return location
#########################################################################################################################
def get_location_by_locationGroup(locationGroupId):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    location= polarisdb["TenantLocations"]
    location = location.find({ 'locationGroup': ObjectId(locationGroupId), 'groupPrimaryLocation': True})
    location = json.loads(dumps(location))    
    return location
#########################################################################################################################
def get_location_by_marketSegment(marketSegmentId):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    location= polarisdb["TenantLocations"]
    location = location.find({ 'marketSegment': ObjectId(marketSegmentId), "marketPrimaryLocation": True})
    location = json.loads(dumps(location))    
    return location
#########################################################################################################################
def get_noaaRegion_by_stateAbbr(location, dict):
    if "address" in location[0]:
        if "stateAbbr" in location[0]['address']:
            sysRegion = get_sysRegion_by_regionAbbr(location[0]['address']["stateAbbr"])
            if sysRegion: 
                if ("regionAbbr" in sysRegion[0]) and ("noaaRegion" in sysRegion[0]):
                    dict['entity']['noaaRegion'] = sysRegion[0]["noaaRegion"]    
    return dict
#########################################################################################################################
def get_locations_noaa_region(dict):
    logging.info("Getting locations noaa region for locationId "+str(dict['entity']['locationId']))
    location = get_location_by_locationId(dict['entity']['locationId'])
    if location:
        dict = get_noaaRegion_by_stateAbbr(location, dict)               
    return dict
#########################################################################################################################    
def get_locationGroup_noaa_region(dict):
    logging.info("Getting locationGroup noaa region for locationGroupId "+str(dict['entity']['locationGroupId']))    
    location = get_location_by_locationGroup(dict['entity']['locationGroupId'])    
    if location: 
        dict = get_noaaRegion_by_stateAbbr(location, dict)
    return dict
#########################################################################################################################
def get_marketSegment_noaa_region(dict):
    logging.info("Getting marketSegment noaa region for marketSegmentId "+str(dict['entity']['marketSegmentId']))    
    location = get_location_by_marketSegment(dict['entity']['marketSegmentId'])    
    if location: 
        dict = get_noaaRegion_by_stateAbbr(location, dict)
    return dict
#########################################################################################################################
def get_default_noaa_region(dict):
    
    tenant = get_tenant(dict['entity']['tenantId'])
    if tenant:
        if "billingAaddress" in tenant[0]:
            if "state" in tenant[0]["billingAaddress"]:
                sysRegion = get_sysRegion_by_region(tenant[0]["billingAaddress"]["state"])
                if sysRegion: 
                    if ("region" in sysRegion[0]) and ("noaaRegion" in sysRegion[0]):
                        dict['entity']['noaaRegion'] = sysRegion[0]["noaaRegion"]                     
    return dict
######################################################################################################################### 

def insert_ingestion_summary_log(dataSourceId, dataSourceName,destinationCollectionId,tenantId, tenantName, ingest_type, netChangePeriod, managed, category, ingestionSchedule,ingestionDate):
    logging.info("inserting ingestion summary log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    if ingest_type == "daily":    
        dict = {"dataSourceId":dataSourceId,"dataSourceName": dataSourceName,"destinationCollectionId":destinationCollectionId,
                "tenantId":tenantId,"tenantName":tenantName,"managed":managed, "category":category, "ingestType":ingest_type,
                "logType":"Ingestion Summary", "ingestionSchedule":ingestionSchedule,
                "status": "Pending",
                "ingestionStartDate":ingestionDate,"ingestionStopDatedate":ingestionDate,"ingestionStopDate":ingestionDate}
    elif ingest_type == "net_change":
        dict = {"dataSourceId":dataSourceId,"dataSourceName": dataSourceName,"destinationCollectionId":destinationCollectionId,
                "tenantId":tenantId,"tenantName":tenantName,"managed":managed, "category":category, "ingestType":ingest_type,
                "logType":"Ingestion Summary", "netChangePeriod":netChangePeriod,"ingestionSchedule":ingestionSchedule,                
                "status": "Pending",
                "ingestionStartDate":ingestionDate,"ingestionStopDate":ingestionDate,"ingestionStopDatedate":ingestionDate}  

    tenantIngestLogSource.insert_one(dict)

#########################################################################################################################
def insert_initial_ingestion_summary_log_for_dbc(dataSourceId, tenantId, ingestType):
    logging.info("inserting ingestion summary log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    initial_ingestion_log = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Ingestion Summary", "status": "completed"}  
    if initial_ingestion_log:
        tenantIngestLogSource.insert_one(initial_ingestion_log)
        print("Updated the initia ingestion log!")

#########################################################################################################################
def insert_ingestion_summary_log_for_engagement_adapters(dataSourceId, dataSourceName, destinationCollectionId,tenantId, tenantName, ingest_type, netChangePeriod, managed, category, ingestionSchedule,ingestionDate):
    logging.info("inserting ingestion summary log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    if ingest_type == "daily":    
        dict = {"dataSourceId":dataSourceId,"dataSourceName": dataSourceName,"destinationCollectionId":destinationCollectionId,
                "tenantId":tenantId,"tenantName":tenantName,"managed":managed, "category":category, "ingestType":ingest_type,
                "logType":"Ingestion Summary", "ingestionSchedule":ingestionSchedule,
                "status": "Pending",
                "ingestionStartDate":ingestionDate,"ingestionStopDatedate":ingestionDate,"ingestionStopDate":ingestionDate}
    elif ingest_type == "net_change":
        dict = {"dataSourceId":dataSourceId,"dataSourceName": dataSourceName,"destinationCollectionId":destinationCollectionId,
                "tenantId":tenantId,"tenantName":tenantName,"managed":managed, "category":category, "ingestType":ingest_type,
                "logType":"Ingestion Summary", "netChangePeriod":netChangePeriod,"ingestionSchedule":ingestionSchedule,                
                "status": "Pending",
                "ingestionStartDate":ingestionDate,"ingestionStopDate":ingestionDate,"ingestionStopDatedate":ingestionDate}  

    tenantIngestLogSource.insert_one(dict)
#########################################################################################################################
def get_tenantIngestLog():
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    return tenantIngestLogSource
#########################################################################################################################
def check_initial_ingestion_summary_log(dataSourceId, tenantId, ingestType):
    logging.info("checking ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Ingestion Summary", "status": "completed"}  
  
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    if ingestLogRecord > 0:
        return True
    else:
        print("---NO INGESTION RECORDS FOUND--")
        return False  
#########################################################################################################################
#fn to check the ingition summary log for the the service titan
def check_initial_ingestion_summary_log_st(dataSourceName, tenantId, ingestType):
    logging.info("checking ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceName":dataSourceName,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Ingestion Summary", "status": "completed"}  
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    if ingestLogRecord > 0:
        return True
    else:
        return False  
    

#########################################################################################################################
def check_initial_ingestion_summary_log_for_dbc(dataSourceId, tenantId, ingestType):
    logging.info("checking ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Ingestion Summary", "status": "completed"}  
  
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    if ingestLogRecord > 0:
        return True
    else:
        print("---NO INGESTION RECORDS FOUND--")
        return False  




#########################################################################################################################
def insert_initial_ingestion_summary_log(dataSourceId, tenantId, ingestType, historyStartDate, historyEndDate):
    logging.info("inserting ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    match_dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Ingestion Summary", "historyStartDate":historyStartDate,"historyEndDate":historyEndDate, "status": "completed",} 

    tenantIngestLogSource.find_one_and_update(match_dict,{"$set":{'historyCompletionDate': datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ')}},upsert=True)
#########################################################################################################################
def insert_initial_ingestion_batch_log(dataSourceId, tenantId, ingestType, historyStartDate, historyEndDate):
    logging.info("inserting ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    if ingestType == "initial":
        dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Historical Batch", "batchStartDate":historyStartDate,"batchEndDate":historyEndDate,                            
            "batchCompletionDate":datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ'), "status": "completed"}            
    tenantIngestLogSource.insert_one(dict)    
#########################################################################################################################
def insert_net_change_ingestion_batch_log(dataSourceId, tenantId, ingestType, netChangeStartDate,netChangeEndDate):
    logging.info("inserting net_change summary log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]        
    if ingestType == "net_change":
        dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":"net_change",
            "logType":"Net Change Batch", "NetChangeBatchStartDate":netChangeStartDate,"NetChangeBatchEndDate":netChangeEndDate,                            
            "NetChangeBatchCompletionDate":datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ'), "status": "completed"}    
    insert_result=tenantIngestLogSource.insert_one(dict)    
    print("Inserted the net change batch deatils with doc  ID:", insert_result.inserted_id)
#########################################################################################################################
def check_net_change_ingestion_batch_log(dataSourceId, tenantId, ingestType):
    logging.info("checking ingestion summary net batch log change log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceId":dataSourceId,"tenantId":tenantId,
            "logType":"Net Change Batch", "status": "completed"} 
  
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    ingestLogRecords = tenantIngestLogSource.find(dict)
    ingestLogRecords = json.loads(dumps(ingestLogRecords))
    if ingestLogRecord > 0:
        recordFlag = True 
    else:
        recordFlag = False  
    return recordFlag, ingestLogRecords
#########################################################################################################################
def check_initial_ingestion_batch_log(dataSourceId, tenantId, ingestType):
    logging.info("checking ingestion summary initial log for "+tenantId)
    client = pymongo.MongoClient(MONGO_URL)

    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    dict = {"dataSourceId":dataSourceId,"tenantId":tenantId, "ingestType":ingestType,
            "logType":"Historical Batch", "status": "completed"}  
  
    ingestLogRecord = tenantIngestLogSource.count_documents(dict)
    ingestLogRecords = tenantIngestLogSource.find(dict)
    ingestLogRecords = json.loads(dumps(ingestLogRecords))
    if ingestLogRecord > 0:
        recordFlag = True 
    else:
        recordFlag = False  
    
    return recordFlag, ingestLogRecords
#########################################################################################################################
def get_history_start_date(tenant, sysDataSourceConfig):    
    # if "dataStartDate" in tenant:
    #     dataStartDate = tenant['dataStartDate']
    # else:
    #     dataStartDate = DEFAULT_DATA_START_DATE

    # tenants having the date as "43831" format and the correct format should be "2023-08-01"
    if "dataStartDate" in tenant and len(tenant["dataStartDate"]) >= 8 :
        dataStartDate = tenant['dataStartDate']
        print("Sending the dataStartDate as history start date")
    else:
        dataStartDate = DEFAULT_DATA_START_DATE
        print("No data start sending the default end date")

    
    if "historicalLimitInMonths"  in sysDataSourceConfig:
        historicalLimitInMonths = sysDataSourceConfig['historicalLimitInMonths']
    else:
        historicalLimitInMonths = int(DEFAULT_HISTORICAL_LIMIT_IN_MONTHS)
   
    dataStartDate = datetime.strptime(dataStartDate, "%Y-%m-%d").date() # Convert str to date
    historicalDateLimit = date.today()-relativedelta(months=historicalLimitInMonths) # Get date based on months\
    print("calculating the historical date limit")
    historyStartDate = max(dataStartDate, historicalDateLimit) # Start history from whichever date is latest
    print("Get the max date out of data start date and history date limit",dataStartDate, historicalDateLimit)

    # print('Tenant Data Start Date:', dataStartDate)
    # print('Data Source Historical Date Limit:', historicalDateLimit)
    return historyStartDate
    
#########################################################################################################################    

def get_last_historical_batch_start_date(tenant, sysDataSourceConfig, ingestType):
    
    recordFlag, ingestLogRecords = check_initial_ingestion_batch_log(sysDataSourceConfig["_id"], tenant["_id"]['$oid'], ingestType)
    NetChangeRecordFlag,NetChangeIngestLogRecords = check_net_change_ingestion_batch_log(sysDataSourceConfig["_id"], tenant["_id"]['$oid'], ingestType)
    batchStartDateList = []
    if recordFlag:
        for record in ingestLogRecords:
            batchStartDateList.append(record['batchStartDate'])
        lastHistoricalBatchStartDate = min(batchStartDateList) 
        lastHistoricalBatchStartDate = datetime.strptime(lastHistoricalBatchStartDate, "%Y-%m-%d")
    elif NetChangeRecordFlag:
        for record in NetChangeIngestLogRecords:
            batchStartDateList.append(record['NetChangeBatchStartDate'])
        lastHistoricalBatchStartDate = min(batchStartDateList) 
        lastHistoricalBatchStartDate = datetime.strptime(lastHistoricalBatchStartDate, "%Y-%m-%d")
    else:
        net_change_period= sysDataSourceConfig.get("netChangePeriod")
        dataFreshnessDelay = sysDataSourceConfig['dataFreshnessDelay'] 
        today_date=datetime.today()
        if net_change_period:
            lastHistoricalBatchStartDate = today_date - timedelta(days=dataFreshnessDelay) - timedelta(days=net_change_period)
        else:
            if '$date' in tenant['createdAt']:
                createdDate = tenant['createdAt']['$date'][:10]
            else:
                createdDate = tenant['createdAt'][:10] 
                print("createdDate",createdDate)
            dataFreshnessDelay = sysDataSourceConfig['dataFreshnessDelay']             
            lastHistoricalBatchStartDate = datetime.strptime(createdDate, "%Y-%m-%d") - timedelta(days=dataFreshnessDelay)            
    return lastHistoricalBatchStartDate                  
#########################################################################################################################  

def get_next_batch_dates(tenant, sysDataSourceConfig, ingestType, historicalBatchUnit, historicalBatchSize):

    historyStartDate = get_history_start_date(tenant, sysDataSourceConfig)

    lastHistoricalBatchStartDate = get_last_historical_batch_start_date(tenant, sysDataSourceConfig, ingestType)   
    
    dataFreshnessDelay = sysDataSourceConfig['dataFreshnessDelay']   

    
    if (historicalBatchUnit is None) or (historicalBatchSize is None):
        historicalBatchUnit = DEFAULT_HISTORICAL_BATCH_UNIT
        historicalBatchSize = DEFAULT_HISTORICAL_BATCH_SIZE
    
    print("getting the dataStart date if it is there if not fetching the created at date\n\n")
    print('History Start Date:\n', historyStartDate)
    print("historicalBatchUnit:\n",historicalBatchUnit)
    print("historicalBatchSize:\n", historicalBatchSize)
    print("Last historical batch start date if the logs are there get it from there lastest record if not take the date which comes current date minus net chnage period minus one day at date\n\n",lastHistoricalBatchStartDate)

    print("calcualting the next batch start date based on the batch size in the airflow....\n\n")
    if historicalBatchUnit == "month":
        nextBatchStartDate = lastHistoricalBatchStartDate - relativedelta(months=int(historicalBatchSize))            
    elif historicalBatchUnit == "day":
        nextBatchStartDate = lastHistoricalBatchStartDate - timedelta(days=int(historicalBatchSize)) 
    print("calculated next batch start date by lastHistorical batch start date minus the batchsize for calls:\n\n",nextBatchStartDate)
    if nextBatchStartDate.date() < historyStartDate:
        nextBatchStartDate = historyStartDate
        historyStartDate = datetime.strftime(nextBatchStartDate, "%Y-%m-%d")
        if '$date' in tenant['createdAt']:
            createdDate = tenant['createdAt']['$date'][:10]
        else:
            createdDate = tenant['createdAt'][:10]
        historyEndDate = datetime.strptime(createdDate, "%Y-%m-%d") - timedelta(days=dataFreshnessDelay)
        historyEndDate = datetime.strftime(historyEndDate, "%Y-%m-%d")    
        insert_initial_ingestion_summary_log(sysDataSourceConfig["_id"], tenant["_id"]['$oid'], ingestType, historyStartDate, historyEndDate)
    nextBatchEndDate = lastHistoricalBatchStartDate - timedelta(days=1)  
    print("calaculated the next batch end date by last Historical Batch StartDate minus 1 day\n",nextBatchEndDate)  
        
    nextBatchStartDate = datetime.strftime(nextBatchStartDate, "%Y-%m-%d") 
    nextBatchEndDate = datetime.strftime(nextBatchEndDate, "%Y-%m-%d")
   
    print("Final nextBatchStartDate, nextBatchEndDate",nextBatchStartDate, nextBatchEndDate)
    return nextBatchStartDate, nextBatchEndDate
#########################################################################################################################    
def getCallsRegexMapping():
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]    
    sysDTSMC = polarisdb["SysDTSMCRules"]
    callsRegexMappings = sysDTSMC.find().sort("evaluationOrder",1)
    callsRegexMappings = json.loads(dumps(callsRegexMappings))
    return callsRegexMappings
#####################################################################################################################
def getCallsSourceMediumCampaign(phone_label,callsRegexMappings):
    
    dict = {}
    dict['source'] =  ''
    dict['medium'] = ''
    dict['campaign'] = ''
    for record in callsRegexMappings:
        if "phoneLabelRegex" in record:
            if re.search(record['phoneLabelRegex'],phone_label,re.IGNORECASE):
                dict['source'] =  record['source']
                dict['medium'] = record['medium']
                dict['campaign'] = record['campaign']
                break
            
    return dict

##################################################################################################################################
def update_pending_status_logs(tenantId, ingestType, DATA_SOURCE_NAME, DAG_NAME, flag, *argv):
    
   sysDataSourceConfig = json.loads(dumps(get_sys_datasource_details(DATA_SOURCE_NAME, DAG_NAME)))
   
   if ingestType in ['daily', 'net_change']:
        tenantIngest_collection = get_tenantIngestLog()
        dtnow = datetime.strptime(datetime.strftime(datetime.now(), "%Y-%m-%d"), "%Y-%m-%d")
        datenowISO =  str(datetime.fromisoformat(datetime.strftime(dtnow, '%Y-%m-%dT%H:%M:%S.%fZ')[:-1]))
        dateP1 = str(datetime.fromisoformat(datetime.strftime(dtnow, '%Y-%m-%dT%H:%M:%S.%fZ')[:-1])+ timedelta(days=1))
        if flag == "failed":
            match_dict = { "$and": [ { "ingestionStartDate": { "$gte": datenowISO } }, { "ingestionStartDate": { "$lt": dateP1 } }, {'tenantId': tenantId},  
                        {"dataSourceId":sysDataSourceConfig[0]["_id"]}, {"ingestType":ingestType},
                        {"logType":"Ingestion Summary"}, {"status": "Pending"}] }                                
                            
            updated_tenantIngestLogs = tenantIngest_collection.update_many(match_dict,{"$set":{'status': "Failed",'ingestionStopDatedate': datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ')}},upsert=True)                
            logging.info("Updated Pending Status Tenant Ingest Summary log for " +tenantId+ "and for " + ingestType)

        if flag == "extract":
            match_dict = { "$and": [ { "ingestionStartDate": { "$gte": datenowISO } }, { "ingestionStartDate": { "$lt": dateP1 } }, {'tenantId': tenantId},  
                            {"dataSourceId":sysDataSourceConfig[0]["_id"]}, {"ingestType":ingestType},
                            {"logType":"Ingestion Summary"}] }
            if len(argv) > 1:
                updated_tenantIngestLogs = tenantIngest_collection.update_many(match_dict,{"$set":{'extractRecordCount': argv[0], 'ingestionStopDatedate': datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ'), "status": "success"}},upsert=True)       
                logging.info("Updated Tenant Ingest Summary log for " +tenantId+ "and for " + ingestType)
            else:
                updated_tenantIngestLogs = tenantIngest_collection.update_many(match_dict,{"$set":{'extractRecordCount': argv[0]}},upsert=True)       
                logging.info("Updated Tenant Ingest Summary log for " +tenantId+ "and for " + ingestType)                
        
        if flag == "transform":
            match_dict = { "$and": [ { "ingestionStartDate": { "$gte": datenowISO } }, { "ingestionStartDate": { "$lt": dateP1 } }, {'tenantId': tenantId},  
                            {"dataSourceId":sysDataSourceConfig[0]["_id"]}, {"ingestType":ingestType},
                            {"logType":"Ingestion Summary"}] }        
            if len(argv) > 1:
                updated_tenantIngestLogs = tenantIngest_collection.update_many(match_dict,{"$set":{'transformRecordCount': argv[0], 'ingestionStopDatedate': datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ'), "status": "success"}},upsert=True)       
                logging.info("Updated Tenant Ingest Summary log for " +tenantId+ "and for " + ingestType)
            else:
                updated_tenantIngestLogs = tenantIngest_collection.update_many(match_dict,{"$set":{'transformRecordCount': argv[0]}},upsert=True)       
                logging.info("Updated Tenant Ingest Summary log for " +tenantId+ "and for " + ingestType) 
            
##########################################################################################################################  
def getBCServices(dependentDataSource,tenantId):

    logging.info("Getting BC Services associated with "+dependentDataSource)
    
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    TenantBcServicesMapping = polarisdb["TenantBcServicesMapping"]
    SysBcServicesCollection = polarisdb["SysBcServices"]  
    SysProductsCollection = polarisdb["SysProducts"]  

    
    product_list = []
    # Get products associated with data source
    products = SysProductsCollection.find({"dataSources.dataSourceId":dependentDataSource})
    product_list = list(products)

    if product_list != []:
        bc_services_list = []
        for product in product_list:
            productId = product['_id']
            bc_services = SysBcServicesCollection.find({"productId":str(productId)})
            bc_services_list.extend(list(bc_services))
        
        # Get unique list of tenants for which the current data source / adapter is applicable based on BC Services mapping
        if bc_services_list != []:
            tenant_bc_service = []
            for bc_service in bc_services_list:
                tenant_bc_service = json.loads(dumps(TenantBcServicesMapping.find({"bcServiceId":str(bc_service['_id']),'status':'Active',"tenantId": tenantId})))
                if tenant_bc_service != []: 
                    return True
            return False
##################################################################################################################################################
def get_dates_by_timezones(client_timezone_id,location_timezone_id,input_date):
    
    utc_timezone = pytz.timezone("UTC")
    date_utc_tz = utc_timezone.localize(input_date)

    client_timezone = pytz.timezone(client_timezone_id["id"]) 
    date_client_tz = date_utc_tz.astimezone(client_timezone) 
    
    location_timezone = pytz.timezone(location_timezone_id["id"]) 
    date_location_tz = date_utc_tz.astimezone(location_timezone)
    return date_client_tz,date_location_tz
###########################################################################################################################################

def get_changed_records_ingestion_dates():    
    date_from = datetime.fromisoformat(datetime.strftime(TODAYS_DATE, '%Y-%m-%dT%H:%M:%S.%fZ')[:-1]) + timedelta(days=-1)
    date_to = datetime.fromisoformat(datetime.strftime(TODAYS_DATE, '%Y-%m-%dT%H:%M:%S.%fZ')[:-1])+timedelta(days=1)
    return date_from, date_to
    
##################################################################################################################################################            

# function to get the data fresheness delay to aggregate the trnaformed engagement calls
def get_dataFreshness_delay_for_transformEngagementcalls_custom_case(data_source_name):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]        
        collection = db["SysDataSources"]
        query_result = collection.find_one({"dataSourceName": data_source_name})
        print("query_result",query_result)
        if query_result:
            json_converted_data = json.dumps(query_result)
            return json_converted_data
        else:
            return {}
    except Exception as e:
        print("error in tne delay",str(e))
        return str(e)
    finally:
        client.close()

# function to get the data fresheness delay to aggregate the trnaformed engagement calls/chats/forms (daily_case)
def get_dataFreshness_delay_for_transformEngagementcalls_daily_case(data_source_name):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]        
        collection = db["SysDataSources"]
        query_result = collection.find_one({"dataSourceName": data_source_name})
        print("query_result",query_result)
        if query_result:
            json_converted_data = json.dumps(query_result)
            return json_converted_data
        else:
            return {}
    except Exception as e:
        print("error in tne delay",str(e))
        return str(e)
    finally:
        client.close()

# function to get the data fresheness delay to aggregate the transformed engagement calls/chats/forms (initial_case)
def get_dataFreshness_delay_for_transformEngagementcalls_initial_case(data_source_name):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]        
        collection = db["SysDataSources"]
        query_result = collection.find_one({"dataSourceName": data_source_name})
        print("query_result",query_result)
        if query_result:
            json_converted_data = json.dumps(query_result)
            return json_converted_data
        else:
            return {}
    except Exception as e:
        print("error in tne delay",str(e))
        return str(e)
    finally:
        client.close()

# function to get the data fresheness delay to aggregate the trnaformed engagement calls
def get_dataFreshness_delay_for_transformEngagementcalls_for_netChange(data_source_name):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]        
        collection = db["SysDataSources"]
        query_result = collection.find_one({"dataSourceName": data_source_name})
        print("query_result",query_result)
        if query_result:
            json_converted_data = json.dumps(query_result)
            return json_converted_data
        else:
            return {}
    except Exception as e:
        print("error in tne delay",str(e))
        return str(e)
    finally:
        client.close()
##################################################################################################################################################        
# function to delete the Data Engagement Calls Aggregated  in custom case
def delete_documents_with_query_for_enagegment_calls_custom(tenant_id,dataSrc_for_cleaning,strat_date_for_cleaning,end_date_for_cleaning):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_cleaning]

        # Create the query using the provided start and end dates
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {"$dateFromString": {"dateString": strat_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    },
                    {
                        "$lt": [
                            "$date",
                            {"$dateFromString": {"dateString": end_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    }
                ]
            }
        }

        # Delete documents that match the specified query
        delete_result = collection.delete_many(query)
        # json_converted_data = json.dumps(delete_result)

        return delete_result.deleted_count
    except Exception as e:
        return str(e)
    finally:
        client.close()

# function to delete the Data Engagement Calls Aggregated  in custom case
def delete_documents_with_query_for_enagegment_calls_daily(tenant_id,dataSrc_for_cleaning,strat_date_for_cleaning,end_date_for_cleaning):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_cleaning]

        # Create the query using the provided start and end dates
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {"$dateFromString": {"dateString": strat_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    },
                    {
                        "$lt": [
                            "$date",
                            {"$dateFromString": {"dateString": end_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    }
                ]
            }
        }

        # Delete documents that match the specified query
        print("strat_date_for_cleaning",strat_date_for_cleaning)
        print("end_date_for_cleaning",end_date_for_cleaning)
        # Find records matching the query
        cursor = collection.find(query)
        print("Before deleting checking the 1st record", cursor[0])

        delete_result = collection.delete_many(query)
        # json_converted_data = json.dumps(delete_result)

        return delete_result.deleted_count
    except Exception as e:
        return str(e)
    finally:
        client.close()

# function to delete the Data Engagement Calls Aggregated  in initial case
def delete_documents_with_query_for_enagegment_calls_initial(tenant_id,dataSrc_for_cleaning,strat_date_for_cleaning,end_date_for_cleaning):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_cleaning]

        # Create the query using the provided start and end dates
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {"$dateFromString": {"dateString": strat_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    },
                    {
                        "$lt": [
                            "$date",
                            {"$dateFromString": {"dateString": end_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    }
                ]
            }
        }

        # Delete documents that match the specified query
        delete_result = collection.delete_many(query)
        # json_converted_data = json.dumps(delete_result)

        return delete_result.deleted_count
    except Exception as e:
        return str(e)
    finally:
        client.close()

# function to delete the Data Engagement Calls Aggregated in net change
def delete_documents_with_query_for_enagegment_calls_netChange(tenant_id,dataSrc_for_cleaning,strat_date_for_cleaning,end_date_for_cleaning):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_cleaning]

        # Create the query using the provided start and end dates
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {"$dateFromString": {"dateString": strat_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    },
                    {
                        "$lt": [
                            "$date",
                            {"$dateFromString": {"dateString": end_date_for_cleaning, "timezone": "UTC"}}
                        ]
                    }
                ]
            }
        }

        # Delete documents that match the specified query
        delete_result = collection.delete_many(query)
        # json_converted_data = json.dumps(delete_result)

        return delete_result.deleted_count
    except Exception as e:
        return str(e)
    finally:
        client.close()

##################################################################################################################################################
#function to perform aggregation on enagagement calls in custom case:
def execute_aggregation_and_insertion_query_for_enagegment_calls_custom_case(dataSrc_for_aggregate,collection_name_to_insert,pipeline):
    try:
        print("dataSrc_for_aggregate",dataSrc_for_aggregate)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_aggregate]
        collection2 = db[collection_name_to_insert]

        # Performing the aggregation
        result_cursor = collection.aggregate(pipeline)
        print("result_cursor",result_cursor)
        # Convert the cursor to a list and print the first 5 documents
        result_list = list(result_cursor)
        converted_arr_dic = [dict(document) for document in result_list]
        print("Number of documents aggregate:", len(result_list))
        for document in result_list:
            print(document)
        # inserting the preaggregation data
        print("converted_arr_dic",len(converted_arr_dic))
        try:
            result_of_inserting = collection2.insert_many(converted_arr_dic)
            print("Inserted document IDs:", result_of_inserting.inserted_ids)
        except Exception as e:
            print("Error inserting documents:", str(e))

        # Close the MongoDB connection
        client.close()

        return result_cursor

    except Exception as e:
        print("An error occurred:", e)
        return []

#function to perform aggregation on enagagement calls in daily case:
def execute_aggregation_and_insertion_query_for_enagegment_calls_daily_case(dataSrc_for_aggregate,collection_name_to_insert,pipeline):
    try:
        print("dataSrc_for_aggregate",dataSrc_for_aggregate)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_aggregate]
        collection2 = db[collection_name_to_insert]

        # Performing the aggregation
        result_cursor = collection.aggregate(pipeline)
        print("result_cursor",result_cursor)
        # Convert the cursor to a list and print the first 5 documents
        result_list = list(result_cursor)
        converted_arr_dic = [dict(document) for document in result_list]
        print("Number of documents aggregate:", len(result_list))
        for document in result_list:
            print(document)
        # inserting the preaggregation data
        print("converted_arr_dic",len(converted_arr_dic))
        try:
            result_of_inserting = collection2.insert_many(converted_arr_dic)
            print("Inserted document IDs:", result_of_inserting.inserted_ids)
        except Exception as e:
            print("Error inserting documents:", str(e))

        # Close the MongoDB connection
        client.close()

        return result_cursor

    except Exception as e:
        print("An error occurred:", e)
        return []

#function to perform aggregation on enagagement calls in initial case:
def execute_aggregation_and_insertion_query_for_enagegment_calls_initial_case(dataSrc_for_aggregate,collection_name_to_insert,pipeline):
    try:
        print("dataSrc_for_aggregate",dataSrc_for_aggregate)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[dataSrc_for_aggregate]
        collection2 = db[collection_name_to_insert]

        # Performing the aggregation
        result_cursor = collection.aggregate(pipeline)
        print("result_cursor",result_cursor)
        # Convert the cursor to a list and print the first 5 documents
        result_list = list(result_cursor)
        converted_arr_dic = [dict(document) for document in result_list]
        print("Number of documents aggregate:", len(result_list))
        for document in result_list:
            print(document)
        # inserting the preaggregation data
        print("converted_arr_dic",len(converted_arr_dic))
        try:
            result_of_inserting = collection2.insert_many(converted_arr_dic)
            print("Inserted document IDs:", result_of_inserting.inserted_ids)
        except Exception as e:
            print("Error inserting documents:", str(e))

        # Close the MongoDB connection
        client.close()

        return result_cursor

    except Exception as e:
        print("An error occurred:", e)
        return []

#function to perform aggregation on enagagement calls in custom case:
def execute_aggregation_and_insertion_query_for_enagegment_calls_netChange(collection_for_aggregate,collection_name_to_insert,pipeline):
    try:
        print("dataSrc_for_aggregate",collection_for_aggregate)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[collection_for_aggregate]
        collection2 = db[collection_name_to_insert]

        # Performing the aggregation
        result_cursor = collection.aggregate(pipeline)
        print("result_cursor",result_cursor)
        # Convert the cursor to a list and print the first 5 documents
        result_list = list(result_cursor)
        converted_arr_dic = [dict(document) for document in result_list]
        print("Number of documents aggregate:", len(result_list))
        # inserting the preaggregation data
        print("converted_arr_dic",len(converted_arr_dic))
        try:
            result_of_inserting = collection2.insert_many(converted_arr_dic)
            print("Inserted document IDs:", result_of_inserting.inserted_ids)
        except Exception as e:
            print("Error inserting documents:", str(e))

        # Close the MongoDB connection
        client.close()

        return len(result_list)

    except Exception as e:
        print("An error occurred:", e)
        return []

##################################################################################################################################################
# fn to find the trnaformed collection in custom case
def execute_aggregation_query_for_enagegment_calls_findTransformQuery_custom_case(collection_name_toFind,tenant_id,strat_date_for_aggreagate,end_date_for_aggreagate):
    # MongoDB connection information
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[collection_name_toFind]

        # Define your query
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {
                                "$dateFromString": {
                                    "dateString": strat_date_for_aggreagate,
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
                                    "dateString": end_date_for_aggreagate,
                                    "timezone": "UTC"
                                }
                            }
                        ]
                    }
                ]
            }
        }
        
        # Execute the query
        result = collection.find(query)
        
        return list(result)
    except Exception as e:
        return str(e)

##################################################################################################################################################
# fn to find the trnaformed collection in custom case
def execute_aggregation_query_for_enagegment_calls_findTransformQuery_netChange(collection_name_toFind,tenant_id,strat_date_for_aggreagate,end_date_for_aggreagate):


    # MongoDB connection information
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[collection_name_toFind]

        # Define your query
        query = {
            "entity.tenantId": tenant_id,
            "$expr": {
                "$and": [
                    {
                        "$gte": [
                            "$date",
                            {
                                "$dateFromString": {
                                    "dateString": strat_date_for_aggreagate,
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
                                    "dateString": end_date_for_aggreagate,
                                    "timezone": "UTC"
                                }
                            }
                        ]
                    }
                ]
            }
        }
        
        # Execute the query
        result = collection.find(query)
        
        return list(result)
    except Exception as e:
        return str(e)


##################################################################################################################################################
# function to insert the ingestion logs of custom case
def insert_into_ingestionLog(insertion_collection, dataSrc_conf_dict, payload_data_dict):
    try:
        print("dataSrc_for_aggregate",insertion_collection)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[insertion_collection]

        logType = ""
        if payload_data_dict['ingest_type'] == "custom":
            logType = "Custom Batch"
        if payload_data_dict['ingest_type'] == "daily":
            logType = "Day-wise Ingest"
        if payload_data_dict['ingest_type'] == "net_change":
            logType = "Net Change Batch"
        if payload_data_dict['ingest_type'] == "initial":
            logType = "Initial Batch"

        

        # Construct the document to be inserted
        document = {
            "dataSourceId": dataSrc_conf_dict["_id"],
            "tenant_id": payload_data_dict['tenant'],
            "batchStartDate": payload_data_dict['ingest_start_date'],
            "batchEndDate": payload_data_dict['ingest_end_date'],
            "dataSourceName": payload_data_dict['data_source_name'],
            "ingest_type": payload_data_dict["ingest_type"],
            "logType": logType,
            "status": "completed",
            "batchCompletionDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        print("document",document)
        # Insert the document into the MongoDB collection
        result = collection.insert_one(document)

        # Check if the insertion was successful
        if result.inserted_id:
            print("Document inserted with ID:", result.inserted_id)
            return result.inserted_id
        else:
            print("Failed to insert document.")
    except Exception as e:
        print("Error inserting document:", str(e))
#  function to insert the ingestion logs for copyGAToGA4
def insert_into_ingestionLog_copyGaToGa4(insertion_collection, dataSrc_conf_dict, payload_data_dict,start_date_obj,end_date_obj):
    try:
        print("dataSrc_for_aggregate",insertion_collection)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[insertion_collection]
        logType = ""

        if payload_data_dict["ingest_type"] == "custom":
            logType="Initial Batch"
        else:
            logType="Custom Batch"

        # Construct the document to be inserted
        document = {
            "dataSourceId": dataSrc_conf_dict["_id"],
            "tenant_id": payload_data_dict['tenant'],
            "batchStartDate": start_date_obj,
            "batchEndDate": end_date_obj,
            "dataSourceName": dataSrc_conf_dict['dataSourceName'],
            "ingest_type": payload_data_dict["ingest_type"],
            "logType": logType,
            "status": "completed",
            "batchCompletionDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        print("document",document)
        # Insert the document into the MongoDB collection
        result = collection.insert_one(document)

        # Check if the insertion was successful
        if result.inserted_id:
            print("Document inserted with ID:", result.inserted_id)
            return result.inserted_id
        else:
            print("Failed to insert document.")
    except Exception as e:
        print("Error inserting document:", str(e))


# function to insert the ingestion logs of daily case
def insert_into_ingestionLog_daily(insertion_collection, dataSrc_conf_dict, payload_data_dict):
    try:
        print("dataSrc_for_aggregate",insertion_collection)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[insertion_collection]

        logType = "Day-wise Ingest"
        # if payload_data_dict['ingest_type'] == "custom":
        #     logType = "Custom Batch"
        # if payload_data_dict['ingest_type'] == "daily":
        #     logType = "Day-wise Ingest"
        # if payload_data_dict['ingest_type'] == "net_change":
        #     logType = "Net Change Batch"
        # if payload_data_dict['ingest_type'] == "initial":
        #     logType = "Initial Batch"

        # match query
        match_query = {
                "tenantId": payload_data_dict['tenant'], 
                "dataSourceId": dataSrc_conf_dict["_id"],
                # "date": payload_data_dict['todayStartDate']
                "$expr": {
                    "$and": [
                        {
                            "$gte": [
                                "$date",
                                {
                                    "$dateFromString": {
                                        "dateString": payload_data_dict['todayStartDate'],
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
                                        "dateString": payload_data_dict['todayEndDate'],
                                        "timezone": "UTC"
                                    }
                                }
                            ]
                        }
                    ]
                }            
        }

        # Delete documents that match the specified query
        delete_result = collection.delete_many(match_query)

        # Check if previous record is deleted
        if delete_result:
            print("Cleanup of previous record was successful")

        # Construct the document to be inserted
        document = {
            "dataSourceId": dataSrc_conf_dict["_id"],
            "tenant_id": payload_data_dict['tenant'],
            "batchStartDate": payload_data_dict['ingest_start_date'],
            "batchEndDate": payload_data_dict['ingest_end_date'],
            "dataSourceName": payload_data_dict['data_source_name'],
            "ingest_type": payload_data_dict["ingest_type"],
            "logType": logType,
            "status": "completed",
            "batchCompletionDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        print("document",document)
        # Insert the document into the MongoDB collection
        result = collection.insert_one(document)

        # Check if the insertion was successful
        if result.inserted_id:
            print("Document inserted with ID:", result.inserted_id)
            return result.inserted_id
        else:
            print("Failed to insert document.")
    except Exception as e:
        print("Error inserting document:", str(e))

##################################################################################################################################################
# function to insert the ingestion logs of net chnage case
def find_ingestion_summary_netChange(collection_to_find, query):
    try:
        print("dataSrc_for_aggregate",collection_to_find,query)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[collection_to_find]
        results_of_query = collection.find(query)
        # Assuming you have a cursor object named results_of_query
        for doc in results_of_query:
            # 'doc' represents each document in the query results
            print(doc)
        print("results_of_query",results_of_query)
        print("collection_to_find",collection_to_find)
        list_of_records = list(results_of_query)


        return len(list_of_records)
    except Exception as e:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
        print("Error find  ingest summary:", str(e))                                                                                                                                                                                                                                                                                                                                                                                                                                                                    


##################################################################################################################################################
#function to do copy and insert operation from DataGoogleAnalyticsPerformanceAggregated to the collections DataGA4ConversionsAggregated,DataGA4TrafficAggregated
def copy_and_insert_records_from_GA_to_GA4(filter_criteria):
    try:
        # MongoDB connection settings
        # print("args in copy putil:",filter_criteria)
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        # Fetch documents from the source collection based on the filter
        source_collection = db["DataGoogleAnalyticsPerformanceAggregated"]
        GA4Conversion_collection_Aggregated = db["DataGA4ConversionsAggregated"]
        GA4Conversion_collection_Traffic_Aggregated = db["DataGA4TrafficAggregated"]
        documents_cursor = source_collection.find(filter_criteria)
        # print("documents_cursor",documents_cursor)
        extracted_documents = list(documents_cursor)
        # Print or process the documents
        # for doc in extracted_documents:
        #     print("doc",doc)
        #     break
        print("extracted_documents count ",len(extracted_documents))

        # creating the empty array for to store the transformed docs
        transformed_dic_for_GA4Conversion = []
        transformed_dic_for_GA4ConversionTrasnformed = []

        # defining the templates for the ga4coversionaggregate and ga4TrafficAggregated
        Template_for_ga4Conversion_aggr = {
            "date": {
                "$date": 0
            },
            "entity": {
                "date": {
                    "$date": 0
                },
                "nDate": 0,
                "tenantId": 0,
                "propertyId": 0,
                "source": 0,
                "medium": 0,
                "campaign": 0,
                "campaignGroup": 0,
                "campaignGroupId": 0,
                "campaignId": 0,
                "channelNameId": 0,
                "channelId": 0,
                "platformId": 0,
                "campaignTypeId": 0,
                "channelTypeName": 0,
                "channelName": 0,
                "platformName": 0,
                "campaignTypeName": 0,
                "locationId": 0,
                "locationGroupId": 0,
                "marketSegmentId": 0,
                "noaaRegion": 0,
                "nielsonRegion": 0,
                "clientRegion": [None],
                "dayOfWeek": 0,
                "hourOfDay": 0
            },
            "conversions": 0,
            "totalRevenue": 0,
            "eventValue": 0,
            "leadsUntaggable": 0,
            "bookingsUntaggable": 0,
            "qualifiedLeadsUntaggable": 0
        }
        
        Template_for_ga4Conversion_traffic_aggr = {
        "date": {
            "$date": 0
        },
        "entity": {
            "date": {
                "$date": 0
            },
            "nDate": 0,
            "tenantId": 0,
            "propertyId":0,
            "source": 0,
            "medium":0,
            "campaign": 0,
            "campaignGroup": 0,
            "campaignGroupId": 0,
            "campaignId": 0,
            "channelNameId": 0,
            "channelId": 0,
            "platformId": 0,
            "campaignTypeId": 0,
            "channelTypeName": 0,
            "channelName": 0,
            "platformName": 0,
            "campaignTypeName": 0,
            "locationId": 0,
            "locationGroupId": 0,
            "marketSegmentId": 0,
            "noaaRegion": 0,
            "nielsonRegion": 0,
            "clientRegion": 0,
            "dayOfWeek": 0,
            "hourOfDay": 0
        },
        "sessions": 0,
        "engagedSessions": 0,
        "newUsers": 0,
        "sessionOtherChannel": 0,
        "LocalTraffic": 0,
        "LocalOrganicTraffic": 0,
        "LocalDirectTraffic": 0,
        "dateClientTz": {
            "$date": 0
        },
        "dateLocationTz": {
            "$date": 0
        },
        "dateUTCTz": {
            "$date": 0
        }
    }
        field_mapping= {
            "sessions": "ga:sessions",
            "newUsers": "ga:newUsers",
        }
        
        # Iterate through the extracted documents
        for document in extracted_documents:
            transformed_document_for_ga4_conversion = {}
            transformed_document_for_ga4_traffic_conversion = {}
            #Mapping the fields as per new GA4  conversionAggr
            for key, value in Template_for_ga4Conversion_aggr.items():
                if key == "sessions" or key == "newUsers":
                    transformed_document_for_ga4_conversion[key] = document[field_mapping.get(key,0)]
                elif key in document:
                    # If the key is present in the source document, use its value
                    transformed_document_for_ga4_conversion[key] = document[key]
                else:
                    # If the key is not present in the source document, set it to 0
                    transformed_document_for_ga4_conversion[key] = 0
            if "date" in transformed_document_for_ga4_conversion:
                    # transformed_document_for_ga4_conversion["date"] = {"$date": transformed_document_for_ga4_conversion["date"]}
                    transformed_document_for_ga4_conversion["date"] = transformed_document_for_ga4_conversion["date"]


            #Mapping the fields as per new GA4 converstionTrafficAggr
            for key, value in Template_for_ga4Conversion_traffic_aggr.items():
                if key == "sessions" or key == "newUsers":
                    transformed_document_for_ga4_traffic_conversion[key] = document[field_mapping.get(key,0)]
                elif key in document:
                    transformed_document_for_ga4_traffic_conversion[key] = document[key]
                else:
                    transformed_document_for_ga4_traffic_conversion[key] = 0
            if "date" in transformed_document_for_ga4_traffic_conversion:
                    # transformed_document_for_ga4_traffic_conversion["date"] = {"$date": transformed_document_for_ga4_traffic_conversion["date"]}
                    transformed_document_for_ga4_traffic_conversion["date"] = transformed_document_for_ga4_traffic_conversion["date"]
            
            transformed_dic_for_GA4Conversion.append(transformed_document_for_ga4_conversion)
            transformed_dic_for_GA4ConversionTrasnformed.append(transformed_document_for_ga4_traffic_conversion)


         #Insert the transformed documents into the target collections
        resultOfG4Conversion = ""
        resultOfG4TrafficAggr=""
        finalResultOfInsertion={}
        if transformed_dic_for_GA4Conversion:
            resultOfG4Conversion=GA4Conversion_collection_Aggregated.insert_many(transformed_dic_for_GA4Conversion)

        if transformed_dic_for_GA4ConversionTrasnformed:
            resultOfG4TrafficAggr=GA4Conversion_collection_Traffic_Aggregated.insert_many(transformed_dic_for_GA4ConversionTrasnformed)
        
        if resultOfG4Conversion  and resultOfG4TrafficAggr:
            finalResultOfInsertion = {
                "result_of_ga4conv_insertion":len(resultOfG4Conversion.inserted_ids),
                "result_of_ga4_Traffic_insertion":len(resultOfG4TrafficAggr.inserted_ids)
            }

        # function  the custom_hash function takes an entity as input, converts it to a string, encodes it into bytes, and then computes the SHA-1(secure hash alorith 1) hash of the encoded data. The result is returned as a hexadecimal string, which represents the hash value of the input entity. This hash value can be used for various purposes, such as comparing data for equality 
        def custom_hash(entity):
            return hashlib.sha1(str(entity).encode()).hexdigest()

        seen_for_collectionAggr = set()
        duplicates_collectionAggr = []
        seen_for_trafficAggr = set()
        duplicates_trafficAggr = []

        for doc in GA4Conversion_collection_Aggregated.find():
            entity = doc["entity"] 
            entity_hash = custom_hash(entity)
            if entity_hash not in seen_for_collectionAggr:
                seen_for_collectionAggr.add(entity_hash)
            else:
                duplicates_collectionAggr.append(doc["_id"])

        for doc in GA4Conversion_collection_Traffic_Aggregated.find():
            entity = doc["entity"]
            entity_hash = custom_hash(entity)
            if entity_hash not in seen_for_trafficAggr:
                seen_for_trafficAggr.add(entity_hash)
            else:
                duplicates_trafficAggr.append(doc["_id"])



        # Create a list of duplicate _id values
        duplicate_ids_conv_aggr = [doc["_id"] for doc in GA4Conversion_collection_Aggregated.find({"_id": {"$in": duplicates_collectionAggr}})]
        duplicate_ids_traffic_aggr = [doc["_id"] for doc in GA4Conversion_collection_Traffic_Aggregated.find({"_id": {"$in": duplicates_trafficAggr}})]

        print("Number of duplicate docs in ga4 collection:",len(duplicate_ids_conv_aggr))
        print("Nmber of duplicates docs in ga4 Traffic :",len(duplicate_ids_traffic_aggr))
        
        if duplicate_ids_conv_aggr:
            # Delete the duplicate documents and capture the result
            resultConversionDuplicateDeletion = GA4Conversion_collection_Aggregated.delete_many({"_id": {"$in": duplicate_ids_conv_aggr}})
            
            # Get the IDs of the deleted documents from the 'deleted_count' attribute of the result
            deleted_document_ids = resultConversionDuplicateDeletion.deleted_count                        
            print("Number of deleted documents ga4 conversion aggr:",deleted_document_ids)
        else:
            print("No duplicate documents to delete in  ga4 conversion aggr.")
        if duplicate_ids_traffic_aggr:
            resultTrafficDuplicateDeletion=GA4Conversion_collection_Traffic_Aggregated.delete_many({"_id": {"$in": duplicate_ids_traffic_aggr}})
            
            deleted_document_ids = resultTrafficDuplicateDeletion.deleted_count            
            print("Number of deleted documents in ga4 traffic aggr:", deleted_document_ids)
        else:
            print("No duplicate documents to delete in ga4 traffic aggr.")
        #  Close the MongoDB connection
        client.close()
        return finalResultOfInsertion
    except Exception as e:
        print("Error in doing copy:",e)

# funciton to isert the service titan api calls data in to the raw collection
def insert_servictian_calls_src_data_into_mongo(callsDataFromApi,ingest_start_date,ingest_end_date, client_tenant_id):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    collectionForSourceCallsData = polarisdb['SourceDataServiceTitanCalls']
    collectionForBatchDetails=polarisdb['BatchNumberDeatils']
    # Convert ingest_start_date and ingest_end_date to datetime objects
    ingest_start_date = datetime.strptime(ingest_start_date, '%Y-%m-%d')
    ingest_end_date = datetime.strptime(ingest_end_date, '%Y-%m-%d')
    ISOStartDate = ingest_start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    ISOEndDate = ingest_end_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    # Delete records within the specified date range to avoid dupliactes
    delete_query = {
        "$and": [
            {'date': {'$gte':ingest_start_date, '$lt':ingest_end_date}},
            {"clientId":str(client_tenant_id)}
        ]
    }
    print("delete query :", delete_query)
    deleted_result = collectionForSourceCallsData.delete_many(delete_query)
    deleted_count = deleted_result.deleted_count
    if deleted_count == 0:
        print("No documents deleted from SourceDataServiceTitanCalls collection.")
    else:
        print(f"{deleted_count} documents deleted from SourceDataServiceTitanCalls collection.")

    result = collectionForSourceCallsData.insert_many(callsDataFromApi)
    countOfRecordsInserted=len(result.inserted_ids)
    # if countOfRecordsInserted is not None and  countOfRecordsInserted > 0:
    #     collectionForBatchDetails.insert_one(batch_detail)
    #     print(f"{countOfRecordsInserted} documents inserted into SourceDataServiceTitanCalls collection successfully.")
    #     return countOfRecordsInserted
    # else:
    #     print("No data to insert into SourceDataServiceTitanCalls.")
    #     return -1

# fucntion to get the records from the source collecton of service titan
def get_service_titan_data_from_src_collection(start_date, end_date, client_id):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    collectionForSourceCallsData = polarisdb['SourceDataServiceTitanCalls']
    query =  {
                "date": {
                    "$gte": start_date,
                    "$lt": end_date
                },
                "clientId": client_id
            }

    projection = {"_id": 0, "batchNumber":0, "batchDate":0}
    print("query",query)
    cursor = collectionForSourceCallsData.find(query,projection)
     # Extracting data into a list of dictionaries
    records = list(cursor)
    if records:
        return  records
    else:
        print("No records found for the client and batch number", client_id)

# fucntion to insert the service titan calls in raw collection:
def insert_servictian_calls_src_data_into_mongo_raw_collection(callsDataFromApi, collection, ingest_type, external_batch_number, client_id, tenant_id):
    print("sample records to insert into mongo raw collection", callsDataFromApi[:10])
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    collectionForSourceCallsData = polarisdb[collection]
    if callsDataFromApi:
        # Query to remove the non-enhanced record from the data
        query = {"batchNumber": external_batch_number, "clientId": client_id}
        raw_src_data_delete_result = collectionForSourceCallsData.delete_many(query)
        countOfRecordsDeleted = raw_src_data_delete_result.deleted_count if raw_src_data_delete_result else 0
        print(f"{countOfRecordsDeleted} non-enhanced documents deleted from {collection} collection.")

        # Insert new documents
        if ingest_type != "net_change":
            result = collectionForSourceCallsData.insert_many(callsDataFromApi)
            countOfRecordsInserted = len(result.inserted_ids)
            print(f"{countOfRecordsInserted} documents inserted into {collection} collection successfully.")
        else:
            for record in callsDataFromApi:
                filter_query = {"entity.date": record['entity']['date'], "entity.tenantId": tenant_id}
                collectionForSourceCallsData.update_one(filter_query, {"$set": record}, upsert=True)
            print("Documents updated in SourceDataServiceTitanCalls.")
    else:
        print(f"No data to insert into {collection}")


# insert the documents into a sample collection:
def insert_servictian_calls_src_data_into_mongo_transform_collection(callsDataFromApi,collection,ingest_type,ingest_start_date,ingest_end_date,tenant_id,match_query):
    print("sample records to insert into mongo collections",callsDataFromApi[:2])
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    collectionForSourceCallsData = polarisdb[collection]
    if callsDataFromApi:
        delete_result=collectionForSourceCallsData.delete_many(match_query['$match'])
        # Printing the number of records deleted
        num_records_deleted = delete_result.deleted_count
        print(f"{num_records_deleted} record(s) deleted trnsformed collection data.")
        result = collectionForSourceCallsData.insert_many(callsDataFromApi)
        countOfRecordsInserted=len(result.inserted_ids)
        print(f"{countOfRecordsInserted} documents inserted into {collection} collection successfully.")
    elif ingest_type == "net_change":
        # for record in callsDataFromApi:
        #     # Filter to identify the document to update based on date and tenant_id
        #     filter_query = {
        #         "entity.date": record['entity']['date'],
        #         "entity.tenantId": tenant_id
        #     }
        #     # Update the specific document that matches the filter
        #     collectionForSourceCallsData.update_one(filter_query, {"$set": record}, upsert=True)
        # print("Documents updated in RawSampleSt.")

        delete_result=collectionForSourceCallsData.delete_many(match_query['$match'])
        # Printing the number of records deleted
        num_records_deleted = delete_result.deleted_count
        print(f"{num_records_deleted} record(s) deleted trnsformed collection data for net.")
        result = collectionForSourceCallsData.insert_many(callsDataFromApi)
        countOfRecordsInserted=len(result.inserted_ids)
        print(f"{countOfRecordsInserted} documents inserted into {collection} collection successfully.")

    else:
        print("No data to insert into SourceDataServiceTitanCalls.")
      
#    service titan ingestion log fn
def insert_ingestion_summary_log_with_config(config_logs,collectionName,category,ingestionSchedule):
    logging.info("inserting ingestion summary log for "+config_logs.get('tenantId'))
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    log_doc = {}
    ingest_type = config_logs.get('ingest_type')
    start_date = datetime.strptime(config_logs.get('ingest_start_date'), "%Y-%m-%d")
    end_date = datetime.strptime(config_logs.get('ingest_end_date'), "%Y-%m-%d")
    delta = timedelta(days=1)  # Define a timedelta of 1 day
    dataSourceId =  config_logs.get('dataSourceName')

    while start_date < end_date:
        ingestion_date = start_date.strftime("%Y-%m-%d")
        logging.info("Ingesting data for date: " + ingestion_date)
        if config_logs.get('ingestType') != "net_change" and config_logs.get('ingestType') == "daily":
            log_doc = {
                "dataSourceId": dataSourceId,
                "dataSourceName":dataSourceId[1:],
                "tenantId": config_logs.get('tenantId'),
                "ingest_start_date": ingestion_date,
                "ingestType": ingest_type,
                "logType": "Day-wise Ingest",
                "status": "success",
                "ingestionStartDate": ingestion_date,
                "ingestionStopDatedate":ingestion_date,
                "destinationCollectionId": collectionName,
                "category":category,
                "ingestionSchedule":ingestionSchedule,
                "managed":config_logs.get('managed'),
                "netChangePeriod":config_logs.get("netChangePeriod"),
                "recordDate":ingestion_date
            }
        elif config_logs.get('ingestType') == "net_change":
            log_doc = {
               "dataSourceId": config_logs.get('dataSourceName'),
                "dataSourceName":dataSourceId[1:],
                "tenantId": config_logs.get('tenantId'),
                "ingest_start_date": ingestion_date,
                "ingestType": ingest_type,
                 "logType": "Ingestion Summary",
                "status": "success",
                "ingestionStartDate": ingestion_date,
                "ingestionStopDatedate":ingestion_date,
                "destinationCollectionId": collectionName,
                "category":category,
                "ingestionSchedule":ingestionSchedule,
                "managed":config_logs.get('managed'),
                "netChangePeriod":config_logs.get("netChangePeriod"),
                "recordDate":ingestion_date
            }
        elif config_logs.get('ingestType') != "net_change" and config_logs.get('ingestType') != "daily":
            log_doc = {
                "dataSourceId": dataSourceId,
                "dataSourceName":dataSourceId[1:],
                "tenantId": config_logs.get('tenantId'),
                "ingest_start_date": ingestion_date,
                "ingestType": ingest_type,
                "logType": "Ingestion Summary",
                "status": "success",
                "ingestionStartDate": ingestion_date,
                "ingestionStopDatedate":ingestion_date,
                "destinationCollectionId": collectionName,
                "category":category,
                "ingestionSchedule":ingestionSchedule,
                "managed":config_logs.get('managed'),
                "netChangePeriod":config_logs.get("netChangePeriod"),
                "recordDate":ingestion_date
            }

        if log_doc:
            tenantIngestLogSource.insert_one(log_doc)
        start_date += delta  # Move to the next day

# fucntion to get the records from the extarct collection of service titan
def get_mongoDb_details ():
    client = pymongo.MongoClient(MONGO_URL)
    db = client[MONGO_DBNAME]
    return db


# Function to retrieve refresh token from DB for MS Ads
def get_refresh_token_from_db():
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sys_data_src_collection = polarisdb["SysDataSources"]
    
    # Retrieve record associated with _MicrosoftAdsCampaignPerformance
    record = sys_data_src_collection.find_one({'_id': '_MicrosoftAdsCampaignPerformance'})
    
    # Extract the refresh token from the record
    refresh_token = record.get('refresh_token', None)
    return refresh_token


# Function to update refresh token in db for MS ads
def update_refresh_token_in_db(new_refresh_token):
    client = pymongo.MongoClient(MONGO_URL)
    polarisdb = client[MONGO_DBNAME]
    sys_data_src_collection = polarisdb["SysDataSources"]
    # Update the refresh token in the record
    sys_data_src_collection.update_one(
        {'_id': '_MicrosoftAdsCampaignPerformance'},
        {'$set': {'refresh_token': new_refresh_token}}
    )

# fn to check the subscription of a tenant to a platform
def check_tenant_subscritption_with_service_titan(tenantId,platform):
    try:
        client = pymongo.MongoClient(MONGO_URL)
        polarisdb = client[MONGO_DBNAME]
        data_src_mapping_collection = polarisdb['TenantDataSourceConfiguration']
        record = data_src_mapping_collection.find_one({'tenantId': tenantId, 'platform': platform})
        return record is not None
    except Exception as e:
        print(f"An error occurred: {e}")
        return False    

# fn to update the to update engagement calls 
def UpdateOneMongodb(collection_name,query,update):
    try:    
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DBNAME]
        collection = db[collection_name]
        print("query1",query)
        print("update1",update)
        result = collection.update_many(query,update)
        print("result",result)
        return result   
    except Exception as e:
        print("Error in updating the record",str(e))
        return str(e)   
    finally:
        client.close()

############################################################################################################################################################################################################
def aggregate_and_insert_fn(foramtted_pipeline_stages,transform_collection,aggregate_collection_name,config,data_source_id,category,ingestionSchedule,netChangePeriod,data_source_platform):
        client = pymongo.MongoClient(MONGO_URL)    
        polarisdb = client[MONGO_DBNAME]
        transform_collection = polarisdb[transform_collection]
        aggregate_collection=polarisdb[aggregate_collection_name]
        tenant_ingest_log= polarisdb[TENANT_INGEST_LOG_COLLECTION]
        print("Started performing aggregation....")
        # Perform aggregation on the transform_collection
        aggregation_result = transform_collection.aggregate(foramtted_pipeline_stages)
        inserted_ids=None
        # Check if there are records in the aggregation result
        if aggregation_result and aggregation_result.alive:
            inserted_ids = []
            for doc in aggregation_result:
                # Generate a new ObjectId for each document to avoid duplicate _id
                doc['_id'] = ObjectId()
                inserted_id = aggregate_collection.insert_one(doc).inserted_id
                inserted_ids.append(inserted_id)
            print("Finished performing aggregation....")
        else:
            print("Aggregation result is empty, skipping insert.")

        
        # Check if any documents were inserted
        if inserted_ids and len(inserted_ids) > 0:
            logging.info("Number of inserted documents: %d", len(inserted_ids))
            
            
            print("config",config)
            # Ingest logs
            if config:
                ingest_type = config.get('ingest_type')
                start_date = datetime.strptime(config.get('ingest_start_date'), "%Y-%m-%d")
                end_date = datetime.strptime(config.get('ingest_end_date'), "%Y-%m-%d")
                delta = timedelta(days=1)  # Define a timedelta of 1 day
                today_date = datetime.now().date()
                tenantDetails  = json.loads(dumps(get_tenant_datasource_details(data_source_platform,config.get('tenant'), aggregate_collection_name)))
                if len(tenantDetails) != 0:
                    tenantDetail = tenantDetails[0]
                    managed=None
                    if "managed" in tenantDetails[0]:
                        managed = tenantDetails[0]['managed']
                    else:
                        managed = False  

                while start_date <= end_date:
                    ingestion_date = start_date.strftime("%Y-%m-%d")

                    # Delete existing log document with the same date and ingest type
                    filter_query = {
                        "dataSourceId": data_source_id,
                        "tenantId": config.get('tenant'),
                        "ingest_start_date": ingestion_date,
                        "ingestType": ingest_type
                    }
                    deleted_count = tenant_ingest_log.delete_one(filter_query).deleted_count
                    
                    log_doc = {
                        "dataSourceId": data_source_id,
                        "dataSourceName":data_source_id[1:],
                        "tenantId": config.get('tenant'),
                        "ingest_start_date": ingestion_date,
                        "ingestType": ingest_type,
                        "logType": "Day-wise Ingest",
                        "status": "success",
                        "recordDate": ingestion_date,
                        "ingestionStartDate": today_date.strftime('%Y-%m-%d'),
                        "ingestionStopDatedate":ingestion_date,
                        "destinationCollectionId": aggregate_collection_name,
                        "category":category,
                        "managed":managed,
                        "ingestionSchedule":ingestionSchedule,
                        "netChangePeriod":netChangePeriod,
                    }
                    if log_doc:
                            tenant_ingest_log.insert_one(log_doc)
                    start_date += delta  # Move to the next day

                print("Updated the audit logs for aggregation")
        else:
            logging.info("No records from the aggregation to insert.")

#fn to net change and initil ingestion is already done or not
def check_initial_or_net_change_ingestion_is_done(ingest_start_date,ingest_end_date,ingest_type,tenantId,sysDataSourceConfig):
    polarisdb = get_mongoDb_details()
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    data_source_id = sysDataSourceConfig.get("_id")
    destination_collections = sysDataSourceConfig.get('destinationCollections', {})
    transform_collection = destination_collections.get('collectionTransformed')

    if ingest_type == "net_change":
        query = {
            "dataSourceId": data_source_id,
            "dataSourceName":data_source_id[1:],
            "destinationCollectionId":transform_collection,
            "tenantId": tenantId,
            "ingestType": ingest_type,
            "ingestionStartDate": {"$gte": ingest_start_date},
            "ingestionStopDatedate": {"$lte": ingest_end_date}
        }
    elif ingest_type == "initial":
        query = {
            "dataSourceId": data_source_id,
            "dataSourceName":data_source_id[1:],
            "destinationCollectionId":transform_collection,
            "tenantId": tenantId,
            "ingestType": ingest_type,
            "batchStartDate": {"$gte": ingest_start_date},
            "batchEndDate": {"$lte": ingest_end_date}
        }
    else:
        # Handle other ingest types or raise an exception
        print("Invalid ingest type provided")

    record = tenantIngestLogSource.find_one(query)
    print("record",record)

    if record:
        return True
    else:
        return False
    
def check_initial_or_net_change_ingestion_is_done_for_tags(ingest_start_date,ingest_end_date,ingest_type,tenantId,sysDataSourceConfig):
    polarisdb = get_mongoDb_details()
    tenantIngestLogSource = polarisdb[TENANT_INGEST_LOG_COLLECTION]
    data_source_id = sysDataSourceConfig.get("_id")
    destination_collections = sysDataSourceConfig.get('destinationCollections', {})
    transform_collection = destination_collections.get('collectionTransformed')
    query=None
    if ingest_type == "net_change":
        query = {
            "dataSourceId": data_source_id,
            "dataSourceName":data_source_id[1:],
            "destinationCollectionId":transform_collection,
            "tenantId": tenantId,
            "ingestType": ingest_type,
            "ingestionStartDate": {"$gte": ingest_start_date},
            "ingestionStopDatedate": {"$lte": ingest_end_date}
        }
    elif ingest_type == "initial":
        query = {
            "dataSourceId": data_source_id,
            "dataSourceName":data_source_id[1:],
            "tenantId": tenantId,
            "ingestType": ingest_type,
            "destinationCollectionId":transform_collection,
            "batchStartDate": {"$gte": ingest_start_date},
            "batchEndDate": {"$lte": ingest_end_date}
        }
    else:
        # Handle other ingest types or raise an exception
        print("Invalid ingest type provided")
    if query:
        record = tenantIngestLogSource.find_one(query)
        print("record",record)

        if record:
            return True
        else:
            return False

def get_tenant_details(tenantId):
    polarisdb = get_mongoDb_details()
    Tenants_collection = polarisdb[TENANTS_COLLECTION]
    if tenantId:
        query = {
            "_id": ObjectId(tenantId),
        }
        tenantDetails = Tenants_collection.find_one(query)
        if tenantDetails:
            return tenantDetails
        else:
            print("NO TENANT FOUND IN THE DB")