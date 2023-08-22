import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from .models import *
##!jupyter nbconvert <notebook name>.ipynb --to html

#!pip3 install pandas==0.20.3
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns


import pymongo
import json
from pymongo import MongoClient
import time
from urllib.parse import quote
from examelk import views, configs

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 51

def malfn(starttime,endtime,daystarttime,slot,un,index,malindex):
    print('mal')
    ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/client.crt','client_key':'/usr/local/share/ca-certificates/elastic/client.key'}

    details = views.getdetails(un)
    print(details)
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'MAL')

    alias1 = []
    try:
        alias1.extend(elastic_client.indices.get_alias(malindex))
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'MAL')    

    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]

    global delyaed_min_login_time
# def startExamDelay():
    
#     delyaed_min_login_time = '2023-03-24T03:00:00.000Z'
#     delyaed_min_login_time = '2022-07-31T05:30:00.000Z'
#     delyaed_min_login_time = '2022-07-31T07:30:00.000Z'
#     delyaed_min_login_time =  2022-07-31T10:00:00.000Z'
    examslot_var = slot

#     delyaed_login_time = '2023-02-26T04:30:00.000Z'
    delyaed_login_time = daystarttime

    
    ### First time it will start from user given TIME, if empty current time will be taken
#     if delyaed_min_login_time == '':
#         now = datetime.now()
#         delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
#     else:
#         delyaed_login_time = delyaed_min_login_time
        

    print("delyaed_login_time : ", delyaed_login_time)    
    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias ) 
    ress = ress.query('range', **{'@timestamp': {'gte':  delyaed_login_time}})
    print("RESS after TIME: ", ress.count())
#     ress = ress.query('match', **{'examslot':examslot_var})
#     ress = ress.query('match', **{'papercode':'NAVIK-NG'})

    res = ress.query('match', **{'malsev':'critical'})
#     print(res.count())
#     res = ress
#     res = res.query('match', **{'centercode':'123'})
    # res = res.query('match', **{'examslot':'C'})
    cnt = res.count()
    print("critical : ", cnt)
    rst = res.execute()
    cnt

    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)

    import time
    start_time = time.time()
    results_df = pd.DataFrame(d.to_dict() for d in res.scan())
    print("shape:: ", results_df.shape)
    amt_df = pd.DataFrame()
    if len(results_df)>=1:
        
        print("time for completion : ",  time.time() - start_time )
        print( "MAL length is", len( results_df['centercode'].unique() ))
        print( " Data frame "  )
    #     results_df.head(2)
    #     if(len(results_df) != 0):
        results_df.head()
        amt_df = results_df[['centercode','regip', 'regmac', 'message', '@timestamp']]
        amt_df = amt_df.rename(columns={'regip':'cip', 'regmac':'cmac', '@timestamp':'column1'})
        
        
        # Extract the desired values
#         event_date = amt_df['message'].split('eventdate:')[1].split(',')[0].strip()
#         mal_ip = amt_df['message'].split('Mal IP:')[1].split(',')[0].strip()
#         amt_df['malip'] = mal_ip.replace('[', '').replace(']','')
#         amt_df['eventdate']=event_date


#         amt_df = results_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        amt_df = amt_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
        amt_records = json.loads(amt_df.T.to_json()).values()
        db.mal.insert_many(amt_records)
        print("MAL : DONE")
        print("time to complete function : ", time.time() - start_time)
    else:
        print("No MAL found")
 
           
        
            ###########################################    
                    #### Login started ####
            ###########################################    
    print("Login started................")        
    ress1= Search(using = elastic_client,  index=alias1 ) 
    ress1 = ress1.query('range', **{'@timestamp': {'gte':  delyaed_login_time}})
    ress1 = ress1.query('match', **{'examslot':examslot_var})    
#     ress1 = ress.query('match', **{'papercode':'NAVIK-NG'})
    res1 = ress1.query('match', **{'msg':configs.start_exam})
    
    
        # rmf_df
    if (((len(results_df) >=1)  | (len(amt_df) >=1 )  ) & (res1.count()>0 )):
      
        print("Login started................")        
#         ress = ress.query('match', **{'examslot':examslot_var})
#         res = ress.query('match', **{'msg':'startExamination'})        

#         res = ress.query('match', **{'msg':'submitAnswer'})
#         res = res.query('match', **{'examslot':'U'})
#         res = ress.query('match', **{'msg':'submitAnswer'})
        print("startExamination count " , res.count())
        res.count()
        
        
        
        login_df = pd.DataFrame(d.to_dict() for d in res1.scan())
        login_df.head()
        login_df = login_df.loc[(login_df['msg'].notnull() & login_df['msg'].str.contains(configs.start_exam) )]        
        login_df.shape
        login_df = login_df.drop_duplicates(subset='candidateid', keep="last")
        login_df.head(2)

        # login_df.head(2)
#         login_df = login_df[["papercode", "column8", "host", "fields", "centercode", "eventdate", "slot", "candidateid", "message",  ]]
        login_df = login_df[[ "column8", "host", "fields", "centercode", "eventdate", "slot", "candidateid", "message",  ]]
        login_df = login_df.rename(columns={"column8": "cip"})
        print(amt_df.head(3))
        print(login_df.head(1))
        amt_can_df = pd.merge(left=amt_df, right=login_df,  how='inner',suffixes=('_mal', '_log'),
                                          on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
        print("MAL CANDIDATES : ", amt_can_df.shape)
        if(len(amt_can_df) >=1):
            amt_records_cand = json.loads(amt_can_df.T.to_json()).values()
            db.matchmal.insert_many(amt_records_cand)
            print("MAL CANDIDATES : DONE")
            print("time to complete function : ", time.time() - start_time)

            print("MAL CONSOLIDATED BEGINS.............")
            consolidate_df = amt_can_df[['slot','centercode','candidateid','cip']]
            consolidate_df['status'] = 'mal'
            consolidate_df.columns = ['examslot','centercode','candidateid', 'cip','status']
            records = json.loads(consolidate_df.T.to_json()).values()
            db.consolidate.insert_many(records)
            print("CONSOLIDATE MAL : DONE")
            print("time to complete function : ", time.time() - start_time)
            error_message = "MAL CONSOLIDATION DONE"
            views.statuslog(un,starttime, endtime, slot, error_message, 'MAL')


        else:
            print("NO candidates in MAL system")
            error_message = "No candidates in MAL system"
            views.statuslog(un,starttime, endtime, slot, error_message, 'MAL')
            
       
            
    else:
        print("Login not executed")
        error_message = "Login not executed"
        views.statuslog(un,starttime, endtime, slot, error_message, 'MAL')
