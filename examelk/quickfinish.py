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
from .models import *
##!jupyter nbconvert <notebook name>.ipynb --to html

#!pip3 install pandas==0.20.3
import pandas as pd
from urllib.parse import quote

# import matplotlib.pyplot as plt
# import seaborn as sns


import pymongo
import json
from pymongo import MongoClient
import time
from datetime import datetime 
import math
from examelk import views, configs
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

def quickfinishfn(starttime,endtime,slot,un,index):
##changes ##cell added
# def quickFinisher():

    details = views.getdetails(un)
    print('quickfinish')
    print(starttime)
    print(details['elkip'],details['elkport'],details['elkuname'],details['mip'],details['mport'],details['mdbname'])
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
        print("=-=-=-=-=-=-",alias)
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message,'Quickfinish')    
    
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]
    
    
    print(" STRAT TIME ::", time.ctime())
    ress= Search(using = elastic_client,  index=alias )
    
    ############## Extract Center-wise make paper available data from ELK
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-07-12T09:40:00.000Z'}})
    ress = ress.query('range', **{'eventdate': {'gte': starttime}})
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-08-29T03:30:00.000Z'}})

#     delyaed_min_login_time = '2021-08-28T03:00:00.000Z'

    
    # ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    ##change 1 #added
    ress = ress.query('match', **{'examslot': slot})


    # ress.count()
    res2 = ress.query('match', **{'message':configs.candidate_submitted})
#     res2 = ress

    # ress1= Search(using = elastic_client,  index=alias )
    # ress1 = ress1.query('range', **{'@timestamp': {'gte': '2020-02-23'}})
    # ress1.count()
    # res2 = ress.query('match', **{'msg':'endExamination'})
#     res2 = res2.query('match', **{'examslot':'C'})
    cnt = res2.count()
    print( "COUNT : ", cnt)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)
    stime = time.time()
    cend_df = pd.DataFrame(d.to_dict() for d in res2.scan())
    print("time : ", time.time() - stime)
    print("SHAPE", cend_df.shape)
    print( cend_df.head(1))
    if(len(cend_df) != 0):
        cend_df =  cend_df.loc[(cend_df['message'].str.contains(configs.candidate_submitted) ) ,]
        print(cend_df.head(1))
        cend_df = cend_df[cend_df['candidateid'].notna()]
#         cend_df =  cend_df.loc[~(cend_df['message'].str.contains("EndExamination") ) ,]
        print("Length After :: ", len(cend_df))
        ######## Data cleansing and Transformation
#         cend_df =  cend_df.loc[(cend_df['message'].str.contains("Candidate Submitted Examination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','slot','centercode', 'papercode', 'column5']]
        print(len(cend_df))
        cend_df =  cend_df.loc[: ,['eventdate', 'message', 'examslot','centercode', 'papercode', 'column5', 'column6', 'sessionid']]
        cend_df['column5'] = cend_df['column5'].str[:-1]        
#         cend_df['sessionid'] = cend_df['column6'].str[:-1]
        # cend_df['column7'] = cend_df['column7'].str[:-1]

        cend_df.head(1)
        print(" candidateid  ??????? ")
        print(cend_df.head)

        ################## Extract Candidate Exam START time
        res1 = ress.query('match', **{'message':configs.start_exam})
        # res1 = res1.query('match', **{'examslot':'C'})
        cnt = res1.count()
        # rst_df = pd.io.json.json_normalize(rst.hits.hits)
        stime = time.time()
        start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
        print("time : ", time.time() - stime)
        start_results_df.shape
        
        
        ######## Info / Extrcat candidate LOGIN details
        clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['message'].str.contains(configs.start_exam) ) ,['candidateid', 'eventdate', 'message', 'examslot','centercode', 'papercode', 'column5','slot', 'column6', 'sessionid']]
#         clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['message'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'message', 'examslot','centercode', 'papercode', 'column5','slot', 'column6', 'sessionid']]

        #         clogin_df.rename(columns = {'column6':'sessionid'})
#         clogin_df['sessionid'] = clogin_df['column6']
        clogin_df.shape
        
        print( "Before group clogin ")
        print(clogin_df.head(20))   
        
        print( "Before group cend_df ")
        print(cend_df.head(2))        

        ####### take first login row - to avaoid repeat candidateid - if Multiple logins
        clogin_df = clogin_df.groupby(['centercode', 'slot', 'sessionid']).first().reset_index()
#         clogin_df = clogin_df.groupby('candidateid').first().reset_index()

        print( "After group clogin ")
        print(clogin_df.head(2)) 


        ##merge clogin_df with cend_df based on slot+papercode+centrecode+sessionID
#         superFinisher_df = pd.merge(left=clogin_df, right=cend_df,  how='right',suffixes=('_clog', '_cend'),
#                                   on= ['sessionid','papercode','examslot', 'centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']

        superFinisher_df = pd.merge(left=clogin_df, right=cend_df,  how='right',suffixes=('_clog', '_cend'),
                                  on= ['sessionid','examslot', 'centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']

        
        print("After JOIN")
        superFinisher_df.head(2)
        ##get SUPER/QUICK FINISHER TIME 
        ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
        #superFinisher_df['end_td']=pd.to_datetime(superFinisher_df['eventdate_clog']).rsub( pd.to_datetime(superFinisher_df['eventdate_cend']) ).astype('timedelta64[m]')
        superFinisher_df['eventdate_clog'] = pd.to_datetime(superFinisher_df['eventdate_clog'])
        superFinisher_df['eventdate_cend'] = pd.to_datetime(superFinisher_df['eventdate_cend'])

        # Calculate time difference in minutes
        time_diff_ns = (superFinisher_df['eventdate_clog'] - superFinisher_df['eventdate_cend']).dt.total_seconds() * 1e9
        minute_td = pd.to_timedelta(1, unit='m')
        superFinisher_df['end_td'] = pd.to_timedelta(time_diff_ns).divide(minute_td)
        # data['Aging'] = data['Created_Date'].rsub(today, axis=0).dt.days
        # superFinisher_df[['end_td', 'eventdate_clog', 'eventdate_cend', 'candidateid_clog', 'centercode', 'examslot', 'papercode']][1:3]


        ##VISUVALIZATION SUPER/QUICK FINISHER
#         superFinisher_df.loc[ superFinisher_df.end_td <15,'end_td_c'] = "<15 min"
#         superFinisher_df.loc[ superFinisher_df.end_td <7,'end_td_c'] = "quick"
        superFinisher_df.loc[ superFinisher_df.end_td <15,'end_td_c'] = "quick"
#         superFinisher_df.loc[ (superFinisher_df.end_td >= 15) & (superFinisher_df.end_td < 30 ),'end_td_c' ] = "15-30 min"
#         superFinisher_df.loc[ (superFinisher_df.end_td >= 7) & (superFinisher_df.end_td <= 15 ),'end_td_c' ] = "fast"
        superFinisher_df.loc[ (superFinisher_df.end_td >= 15) & (superFinisher_df.end_td <= 30 ),'end_td_c' ] = "fast"
#         superFinisher_df.loc[superFinisher_df.end_td >15 , 'end_td_c' ] ="ontime"
        superFinisher_df.loc[superFinisher_df.end_td >30 , 'end_td_c' ] ="ontime"       
        
              
#         superFinisher_df.loc[(superFinisher_df.end_td >=30 ) & (superFinisher_df.end_td <=45 ), 'end_td_c' ] ="30-45 min"
#         superFinisher_df.loc[(superFinisher_df.end_td >=45 ) & (superFinisher_df.end_td <=60 ), 'end_td_c' ] ="45-60 min"
#         superFinisher_df.loc[ superFinisher_df.end_td >60,'end_td_c'] = "Beyond 1 Hr"
        superFinisher_df.loc[superFinisher_df.end_td.isnull(), 'end_td_c' ] = "Not Finished"


        print(superFinisher_df.head(2))

        ########### inserting into MongoDB database
        # record1 = db['qpattern']
        ##drop
#         db.qfinisher.drop()
        ##Insert
        records = json.loads(superFinisher_df.T.to_json()).values()
        db.qfinisher.insert_many(records)
        print("QUICK FINISHER - DONE")
        print("time QUICK FINISHER : ", time.time() - stime)

        consolidate_df = superFinisher_df.loc[superFinisher_df.end_td_c=='quick', ('examslot','centercode','candidateid') ]
        consolidate_df['status'] = 'quick finish'
        consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
        records = json.loads(consolidate_df.T.to_json()).values()
        db.consolidate.insert_many(records)
        print("CONSOLIDATE - DONE")
        print("time CONSOLIDATE QUICK FINISHER : ", time.time() - stime)
        error_message = "QuickFinish Successfully Completed"
        views.statuslog(un,starttime,endtime,slot,error_message, 'QUICKFINISH')

#         time.sleep(120)
        

    else:
        print("NO ONE SUBMITED EXAM")
        error_message = "NO ONE SUBMITTED EXAM!"
        views.statuslog(un,starttime,endtime,slot,error_message, 'QUICKFINISH')

#         time.sleep(20)
  


# In[21]:


# superFinisher_df


# In[ ]:


# quickFinisher()
# while True:
#     quickFinisher()


# ## WITH CANDIDATEID in endExamination 

# In[ ]:


##changes ##cell added
# def quickFinisher():
    
#     ############## Extract Center-wise make paper available data from ELK
#     ress= Search(using = elastic_client,  index=alias )
#     # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-03-18T04:15:00.000Z'}})
#     # ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
#     ##change 1 #added
#     ress = ress.query('match', **{'examslot':'A'})

#     # ress.count()
#     res2 = ress.query('match', **{'message':'Candidate Submitted Examination'})

#     # ress1= Search(using = elastic_client,  index=alias )
#     # ress1 = ress1.query('range', **{'@timestamp': {'gte': '2020-02-23'}})
#     # ress1.count()
#     # res2 = ress.query('match', **{'msg':'endExamination'})
# #     res2 = res2.query('match', **{'examslot':'C'})
#     cnt = res2.count()
#     print( "COUNT : ", cnt)
#     # rst_df = pd.io.json.json_normalize(rst.hits.hits)
#     stime = time.time()
#     cend_df = pd.DataFrame(d.to_dict() for d in res2.scan())
#     print("time : ", time.time() - stime)
#     print("SHAPE", cend_df.shape)
#     print( cend_df.head(1))

#     if(len(cend_df) != 0):
#         ######## Data cleansing and Transformation
#         # cend_df =  cend_df.loc[(cend_df['message'].str.contains("Candidate Submitted Examination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','slot','centercode', 'papercode', 'column5']]
#         print(len(cend_df))
#         cend_df =  cend_df.loc[: ,['eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5']]
#         cend_df['column5'] = cend_df['column5'].str[:-1]
#         # cend_df['column7'] = cend_df['column7'].str[:-1]

#         cend_df.head(1)
#         print(" candidateid  ??????? ")
#         print(cend_df.head)

#         ################## Extract Candidate Exam START time
#         res1 = ress.query('match', **{'msg':'startExamination'})
#         # res1 = res1.query('match', **{'examslot':'C'})
#         cnt = res1.count()
#         # rst_df = pd.io.json.json_normalize(rst.hits.hits)
#         stime = time.time()
#         start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
#         print("time : ", time.time() - stime)
#         start_results_df.shape

#         ######## Info / Extrcat candidate LOGIN details
#         clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['msg'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5','slot']]
#         clogin_df.shape

#         ####### take first login row - to avaoid repeat candidateid - if Multiple logins
#         clogin_df = clogin_df.groupby('candidateid').first().reset_index()

#         print( "clogin ",  clogin_df.shape)


#         ##merge clogin_df with cend_df based on slot+papercode+centrecode+sessionID
#         superFinisher_df = pd.merge(left=clogin_df, right=cend_df,  how='right',suffixes=('_clog', '_cend'),
#                                   on= ['papercode','examslot','column5', 'centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']

#         superFinisher_df.head(2)
#         ##get SUPER/QUICK FINISHER TIME 
#         ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
#         superFinisher_df['end_td']=pd.to_datetime(superFinisher_df['eventdate_clog']).rsub( pd.to_datetime(superFinisher_df['eventdate_cend']) ).astype('timedelta64[m]')
#         # data['Aging'] = data['Created_Date'].rsub(today, axis=0).dt.days
#         # superFinisher_df[['end_td', 'eventdate_clog', 'eventdate_cend', 'candidateid_clog', 'centercode', 'examslot', 'papercode']][1:3]


#         ##VISUVALIZATION SUPER/QUICK FINISHER
# #         superFinisher_df.loc[ superFinisher_df.end_td <15,'end_td_c'] = "<15 min"
#         superFinisher_df.loc[ superFinisher_df.end_td <7,'end_td_c'] = "quick"
# #         superFinisher_df.loc[ (superFinisher_df.end_td >= 15) & (superFinisher_df.end_td < 30 ),'end_td_c' ] = "15-30 min"
#         superFinisher_df.loc[ (superFinisher_df.end_td >= 7) & (superFinisher_df.end_td <= 15 ),'end_td_c' ] = "fast"
#         superFinisher_df.loc[superFinisher_df.end_td >15 , 'end_td_c' ] ="ontime"
              
# #         superFinisher_df.loc[(superFinisher_df.end_td >=30 ) & (superFinisher_df.end_td <=45 ), 'end_td_c' ] ="30-45 min"
# #         superFinisher_df.loc[(superFinisher_df.end_td >=45 ) & (superFinisher_df.end_td <=60 ), 'end_td_c' ] ="45-60 min"
# #         superFinisher_df.loc[ superFinisher_df.end_td >60,'end_td_c'] = "Beyond 1 Hr"
#         superFinisher_df.loc[superFinisher_df.end_td.isnull(), 'end_td_c' ] = "Not Finished"


#         print(superFinisher_df.head(2))

#         ########### inserting into MongoDB database
#         # record1 = db['qpattern']
#         ##drop
# #         db.qfinisher.drop()
#         ##Insert
#         records = json.loads(superFinisher_df.T.to_json()).values()
#         db.qfinisher.insert_many(records)
#         print("QUICK FINISHER - DONE")
#         print("time QUICK FINISHER : ", time.time() - stime)
#         time.sleep(120)
#     else:
#         print("NO ONE SUBMITED EXAM")
#         time.sleep(20)


# # In[ ]:


# # quickFinisher()
# while True:
#     quickFinisher()


# # In[ ]:


# test = db.qfinisher.aggregate([
 
#   { "$group": { 
#     "_id": { "name": "$candidateid"}, 
#     "dups": { "$addToSet": "$_id" }, 
#     "count": { "$sum": 1 } 
#   }}, 
#   { "$match": { 
#     "count": { "$gt": 1 } 
#   }}
# ])# .forEach(function(doc){
#     doc.dups.shift();      
#     doc.dups.forEach( function(dupId){ 
#         duplicates.push(dupId);
#         }
#     )    
# })


# In[ ]:





# In[ ]:


# test.next()


# ### Run this in mongodb

# In[ ]:


# future use
# var duplicates = [];
# db.qfinisher.aggregate([
 
#   { $group: { 
#     _id: { name: "$candidateid", examsolt: "$examslot"}, 
#     dups: { "$addToSet": "$_id" }, 
#     count: { "$sum": 1 } 
#   }}, 
#   { $match: { 
#     count: { "$gt": 1 } 
#   }}
# ],
# {allowDiskUse: true}    
# ).forEach(function(doc) {
#     doc.dups.shift();      
#     doc.dups.forEach( function(dupId){ 
#         duplicates.push(dupId);
#         }
#     )    
# })

# (/, If, you, want, to, Check, all, "_id", which, you, are, deleting, else, print, statement, not, needed)
# printjson(duplicates);     


# (/, Remove, all, duplicates, in, one, go)
# db.qfinisher.remove({_id:{$in:duplicates}})
