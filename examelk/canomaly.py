#!/usr/bin/env python
# coding: utf-8

# In[1]:


# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = "all"


# In[3]:


import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import datetime
from .models import Error
##!jupyter nbconvert <notebook name>.ipynb --to html
from examelk import views
#!pip3 install pandas==0.20.3
import pandas as pd
from urllib.parse import quote
from examelk import configs
# import matplotlib.pyplot as plt
# import seaborn as sns


import pymongo
import json
from pymongo import MongoClient
import time
import math
from examelk import views
# from examelk.views import ES_CERTIFICATES


pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 51


# # In[4]:


# # ###sudo update-ca-certificates
ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/client.crt','client_key':'/usr/local/share/ca-certificates/elastic/client.key'}

# elastic_client = Elasticsearch(host='iafems.cdacchn.in', port=9200, verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=('elastic','34ERdfcv@34'),timeout=300)


# # In[52]:


# ##gaierror: [Errno -2] Name or service not known /etc/hosts
# ###10.184.53.57    iafems.cdacchn.in
# alias = []
# # alias.extend(elastic_client.indices.get_alias('iafstar-june-testing'))

# alias.extend(elastic_client.indices.get_alias('iafafcat-aug-test-20210829'))


# alias


# # In[10]:


# from urllib.parse import quote
# ################ MONGODB conaction details
# # mongod --bind_ip 10.184.51.194 --dbpath /var/lib/mongodb/

# # connection = MongoClient("mongodb://10.184.51.194:27017")
# connection = MongoClient("mongodb://10.184.61.202:27017")

# db=connection['afcat21Aug']


# # In[11]:


# db


# # ## . Super Finisher 
# # #### candidate who finish exam early
# # 
# # - only few candidates are submitted exam (only 14K in all 4 slots)
# #     - candidate_id is missing in the Log
# # - mostly exam ended by CI, 
# #     - candidate_id is missing in the Log

# # ## WITH OUT CANDIDATEID in endExamination 

# # In[ ]:


# ########## NEW ################
# import math 

# -(.36*math.log(.36,2) ) - (.64 * math.log(.64,2))
# # -(.36 math.log(14,5)) - (.64 math.log(.64,2))


# # In[54]:

def canomalyfn(starttime,endtime,qendtime,slot,un,index,ansindex):
##changes ##cell added
# def quickFinisher():
    print('canomaly')
    print(starttime,qendtime,slot,index)
    details = views.getdetails(un)
    print(slot)
    print("=-=--=-=",details)
    
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(ansindex))
        print("=-=-=-=-=-=-",alias)
    except:
        error_message="NO Index FOund"
        views.statuslog(un,starttime,endtime,slot,error_message, 'Canomaly')
        

        
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]
    import time
    stime = time.time()
    print(" STRAT TIME ::", time.ctime())
    ress= Search(using = elastic_client,  index=alias )
    
    ############## Extract Center-wise make paper available data from ELK
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-07-12T09:40:00.000Z'}})
    delyaed_min_login_time = starttime
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-08-29T03:30:00.000Z'}})
    delyaed_login_time_min = qendtime
#     delyaed_min_login_time = '2021-08-28T03:00:00.000Z'

    
    # ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    ##change 1 #added
    examSlotVar = slot

    print(examSlotVar)
    # ress.count()
    if delyaed_min_login_time == '':
        now = datetime.now()
        delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
    else:
        delyaed_login_time = delyaed_min_login_time
    print(delyaed_login_time)
    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias )
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    # ress = ress.query('range', **{'eventdate': {'gte': '2020-03-09T00:00:00.000Z'}})

    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min }})
    ##change 1 #added
    # delyaed_min_login_time = '2020-11-04T3:50:00.000Z'
    # delyaed_min_login_time_l = '2020-11-04T11:00:00.000Z'
    # ress = ress.query('range', **{'eventdate': {'lte':  delyaed_min_login_time_l}})
    print(" NO MSG :: ", ress.count())
    print('------------------------------------------------------------------------------------------------------',ress)

    ress = ress.query('match', **{'msg':configs.submit_answer})
    #     res = ress.query('match', **{'makePaperSectionAvailable'})

    print(" NO SLOT :: ", ress.count())
    # ress = ress.query('match', **{'examslot':'A'})
    # ress = ress.query('match', **{'centercode':'400'})
    # res = ress.query('match', **{'papercode':'YY'})

    res = ress

    import datetime as dt
    from dateutil.parser import parse


    import time
    start_time = time.time()
    print("start time : ", start_time)
    results_df = pd.DataFrame(d.to_dict() for d in ress.scan())
    print(results_df.shape)
    print("time for completion : ",  time.time() - start_time )
    print("Data Frame ", results_df.head())
    if len(results_df) != 0:
        print("Unique Center : ", len(results_df['centercode'].unique() ))
        print("UNIQUE CANDIDATE : ", len(results_df['candidateid'].unique() ))

        results_df.columns
        results_df.head(1)
        #### set Minimum Time to crawl NEXT time
        delyaed_min_login_time = results_df['eventdate'].min()


        ans_df = results_df.copy()


        print("ans_df : ", ans_df.shape)
        print("ans_df : ", ans_df['centercode'].unique() )



        import numpy as np
        # results_df.head(1)
        # ans_df.head(1)
        from dateutil.parser import parse
        eventedate_date = [parse(d) for d in results_df['eventdate'] ]
        results_df['eventdate_date'] = eventedate_date
        results_df = results_df.sort_values(['candidateid', 'eventdate'], ascending=True)

        # results_df.groupby('column8')['eventdate_date'].diff()

        # results_df['timeToAns'] = results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')
        results_df['timeToAns'] = results_df.groupby('candidateid')['eventdate_date'].diff() /  np.timedelta64(1, 's')

        ans_df = results_df.copy()

        # ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)
        ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'q2', '@version','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)

        ## find the avge time for any 10 guestions, in this case first 9 question mean/average time would be null. 
        # df[df['b'].notnull()]
        ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].mean().droplevel(level='candidateid')

        ans_df['avgTime_ten']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].mean().droplevel(level='candidateid')

        # df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
        # rdf.groupby(['no']).rolling(5)['s'].mean()

        ## 15 questoin answerd with in 3 min(180 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
        ans_df.loc[ (ans_df.avgTime <= 12),  'anomaly' ] = 1
        ans_df.loc[ (ans_df.avgTime > 12),  'anomaly' ] = 0

        ## 10 questoin answerd with in 2 min(120 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
        ans_df.loc[ (ans_df.avgTime_ten <= 12),  'anomaly_ten' ] = 1
        ans_df.loc[ (ans_df.avgTime_ten > 12),  'anomaly_ten' ] = 0
        from pytz import timezone
        ## Convert UST to IST
        isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
        ans_df['eventdate_ist'] = isttime
        st = time.time()
        ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)


        # ########### inserting into MongoDB database
        # record1 = db['qpattern']
        # # ##drop
        # db.anomaly.drop()
        # # ##Insert
        records = json.loads(ans_df.T.to_json()).values()
        db.anomaly.insert_many(records)
        print("ANOMALY DETECTION : DONE")
        print("time to complete function : ", time.time() - stime)

        consolidate_df = ans_df.loc[ans_df.anomaly==1, ('examslot','centercode','candidateid') ]
        consolidate_df['status'] = 'fast answering'
        consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
        records = json.loads(consolidate_df.T.to_json()).values()
        db.consolidate.insert_many(records)
        print("CONSOLIDATE ::: ANOMALY DETECTION : DONE")
        print("time to complete function : ", time.time() - stime)
        error_message = "Canomaly Successfully Completed"
        views.statuslog(un,starttime,endtime,slot,error_message, 'Canomaly')
    else:
        print("NO - ANOMALY DETECTION DONE")
        error_message = "NO - ANOMALY DETECTION DONE"
        views.statuslog(un,starttime,endtime,slot,error_message, 'Canomaly')