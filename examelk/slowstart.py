#!/usr/bin/env python
# coding: utf-8

# In[1]:


# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = "all"


# In[2]:


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

def slowstartfn(starttime,endtime,slot,un,index):
    print('slowstart')
    ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/client.crt','client_key':'/usr/local/share/ca-certificates/elastic/client.key'}

    details = views.getdetails(un)
    print(details)
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'SlowStart')    

    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]
    
    pd.set_option('display.max_rows', 500)
    from datetime import datetime
    global delyaed_min_login_time
    # def startExamDelay():

    #     delyaed_min_login_time = '2021-02-12T00:00:00.000Z'

    delyaed_min_login_time = starttime
    #     delyaed_min_login_time = '2021-08-29T03:30:00.000Z'
    examSlotVar = slot

    ### First time it will start from user given TIME, if empty current time will be taken
    if delyaed_min_login_time == '':
        now = datetime.now()
        delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
    else:
        delyaed_login_time = delyaed_min_login_time

    print("delyaed_login_time : ", delyaed_login_time)    
    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias )
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    #     ress = ress.query('range', **{'eventdate': {'gte': '2021-03-15T010:15:00.00'}})
    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    ress.count()
    ##change 1 #added
    #     res = ress.query('match', **{'msg':'SUCCESS: Make Paper Section Available'})
    res = ress.query('match', **{'msg':configs.make_paper})
    res.count()
    #     res = ress.query('fuzzy', **{'msg':{'value':"SUCCESS: Make Paper Available"}})

    #     res = ress.query('match', **{'makePaperSectionAvailable'})
    res = res.query('match', **{'examslot': slot})
    res.count()
    #     res = res.query('match', **{'centercode':'123'})
    #     res = res.query('match', **{'examslot':'C'})
    cnt = res.count()
    rst = res.execute()
    cnt


    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)




    import time
    start_time = time.time()
    results_df = pd.DataFrame(d.to_dict() for d in res.scan())
    print("shape:: ", results_df.shape)
    print("time for completion : ",  time.time() - start_time )
    if(len(results_df) != 0):
        print( "length is", len( results_df['centercode'].unique() ))
        print( " Data frame ", results_df.head(2) )

        #### set Minimum Time to crawl NEXT time
        delyaed_min_login_time = results_df['eventdate'].min()

        mpa_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains(configs.make_paper) ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]
        if(len(mpa_df) != 0):
                mpa_df = mpa_df[['msg','centercode','eventdate', 'papercode', 'examslot']]

                print("RESULTS DF ")
                print(results_df.head(1))

                # results_df.head(1)
                ############## Data Cleansing and Transformation.
                mpa_df.head(1)
        #     mpa_df['papercode'] = results_df.column4.str[-3:-1]
                print(mpa_df.head(3))
                #     mpa_df['column7'] = results_df.column7.str[:-1]



                ############EACH CENTER has multiple make paper available for SAME paper/slot
                ## SO Ihave grouped and takem=n MAX time-- may be its test data not in actual I GUESS
                idx = mpa_df.groupby(['centercode', 'papercode','examslot'])['eventdate'].transform(max) == mpa_df['eventdate']
                
                
                mpa_df = mpa_df[idx]
                print("head 2")
                print(mpa_df.head(2))
                print('Make paper centercode')
                print(mpa_df['centercode'])


                ################## Extract Candidate Exam START time
                res1 = ress.query('match', **{'msg':configs.start_exam})
                # res1 = res1.query('match', **{'examslot':'C'})
                cnt = res1.count()
                print("RES1 CNT : ", cnt, ress.count())
                # rst_df = pd.io.json.json_normalize(rst.hits.hits)
                stime = time.time()
                start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
                print("time : ", time.time() - stime)
                print("SHAPE : start Examination : ", start_results_df.shape)
                print(start_results_df.columns)
                
        #         start_results_df.rename(columns = {'exampapercode':'papercode'},inplace=True )
                ######## Info / Extrcat candidate LOGIN details
                clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & 
                                                start_results_df['msg'].str.contains(configs.start_exam) ) ,['candidateid', 'eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5','slot']]
                clogin_df.shape

                ####### take first login row - to avaoid repeat candidateid - if Multiple logins
                #     clogin_df = clogin_df.groupby('candidateid').first().reset_index()
        #         clogin_df = clogin_df.groupby('candidateid').last().reset_index()
                clogin_df = clogin_df.groupby('candidateid').first().reset_index()
                print("clogin_df : shape ", clogin_df.shape)
                print(clogin_df.head(3))
                print("Login Centercode : ")
                print(clogin_df['centercode'])
                ##merge mpa_df with clogin_df based on slot+papercode+centrecode
                sexm_tdiff_df = pd.merge(left=mpa_df, right=clogin_df,  how='left',suffixes=('_mpa', '_clog'),
                                        on= ['papercode','examslot','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
                print("sexm_tdiff_df :: ", sexm_tdiff_df.shape)
                print("differnce Centercode : ")
                print(sexm_tdiff_df['centercode'])
                print(sexm_tdiff_df.head(3))
                sexm_tdiff_df = sexm_tdiff_df.loc[sexm_tdiff_df['candidateid'].notnull(),]
                
                print("WIth Candiate ID :" , sexm_tdiff_df.shape)
                ################ get ACTUALL TIME DIFFERENCE for Candidate LOGIN
                ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
                #sexm_tdiff_df['start_td']=pd.to_datetime(sexm_tdiff_df['eventdate_mpa']).rsub( pd.to_datetime(sexm_tdiff_df['eventdate_clog']) ).astype('timedelta64[m]')
                sexm_tdiff_df['eventdate_clog'] = pd.to_datetime(sexm_tdiff_df['eventdate_clog'])
                sexm_tdiff_df['eventdate_mpa'] = pd.to_datetime(sexm_tdiff_df['eventdate_mpa'])

                # Calculate time difference in minutes
                time_diff_ns = (sexm_tdiff_df['eventdate_clog'] - sexm_tdiff_df['eventdate_mpa']).dt.total_seconds() * 1e9
                minute_td = pd.to_timedelta(1, unit='m')
                sexm_tdiff_df['start_td'] = pd.to_timedelta(time_diff_ns).divide(minute_td)
                sexm_tdiff_df[['start_td', 'eventdate_clog', 'eventdate_mpa', 'candidateid', 'centercode', 'examslot', 'papercode']][1:3]
                print("Is it Empty ??")
                print(sexm_tdiff_df.head(5))

                ############ Time cluster 
        #         sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td  == 0) ,'start_td_c' ] = "< 1 min"
                sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td  == 0) ,'start_td_c' ] = "ontime"

        #         sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td   == 1) ,'start_td_c' ] = "1- 2 min"
        #         sexm_tdiff_df.loc[ sexm_tdiff_df.start_td == 2,'start_td_c'] = '2-3 min'
                sexm_tdiff_df.loc[(sexm_tdiff_df.start_td ==1 ) | (sexm_tdiff_df.start_td   == 2 )| (sexm_tdiff_df.start_td   == 2 ), 'start_td_c' ] ="slow"
                sexm_tdiff_df.loc[sexm_tdiff_df.start_td.isnull(), 'start_td_c' ] = 'Not started'
                # sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "Issues"
        #         sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "< 1 min"
        #         sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td >= 5 ),  'start_td_c' ] = "> 5 min"
        #         sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td >= 4 ),  'start_td_c' ] = "delayed"
                sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td >= 3 ),  'start_td_c' ] = "delayed"




                # sexm_tdiff_df


                ########### inserting into MongoDB database
                # record1 = db['qpattern']
                ##drop
        #         db.starttime.drop()
                ##Insert
                records = json.loads(sexm_tdiff_df.T.to_json()).values()
                db.starttime.insert_many(records)
                print("START TIME DELAY : DONE")
                print("time to complete function : ", time.time() - stime)
                
                consolidate_df = sexm_tdiff_df.loc[sexm_tdiff_df.start_td_c=='delayed', ('examslot','centercode','candidateid') ]
                consolidate_df['status'] = 'start delay'
                consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
                records = json.loads(consolidate_df.T.to_json()).values()
                print(records)
                db.consolidate.insert_many(records)
                print("CONSOLIDATE : DONE")
                print("time to complete function : ", time.time() - stime)
                #         time.sleep(3000)
                error_message = "SlowStart Successfully Completed"
                views.statuslog(un,starttime,endtime,slot,error_message, 'SlowStart')

        else:
                print("CANDIDATE NOT STARTED EXAM")
                error_message = "CANDIDATE NOT STARTED EXAM "
                views.statuslog(un,starttime,endtime,slot,error_message, 'SlowStart')
    else:
        print("START TIME DELAY NOT DONE!!")
        error_message = "START TIME DELAY NOT DONE!"
        views.statuslog(un,starttime,endtime,slot,error_message, 'SlowStart')

