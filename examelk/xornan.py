import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
##!jupyter nbconvert <notebook name>.ipynb --to html
from .models import *
#!pip3 install pandas==0.20.3
import pandas as pd
from urllib.parse import quote

# import matplotlib.pyplot as plt
# import seaborn as sns


import pymongo
import json
from pymongo import MongoClient
import time
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

def xornanfn(starttime, endtime, qstarttime,qendtime,slot,un,index):
##changes ##cell added
# def quickFinisher():
    print('xornan')
    details = views.getdetails(un)
    
    print("=-=--=-=",details)
    
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
        print("=-=-=-=-=-=-",alias)
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'Xornan')
    
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]

    global delyaed_min_login_time

    ############## based on the time , extract all the document having cmac field in it.
    ress= Search(using = elastic_client,  index=alias )     

    # delyaed_login_time     = '2022-03-29T00:30:00.000Z'
    # delyaed_login_time_min = '2022-03-29T05:00:00.000Z'

    # delyaed_login_time     = '2023-01-23T02:30:00.000Z'
    # delyaed_login_time_min = '2023-01-23T04:50:00.000Z'

    delyaed_login_time     = qstarttime
    delyaed_login_time_min = qendtime
    # examslot_var = 'B'

    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min, }})
    ress.count()
    # ress = ress.query('exists',**{'field' :'cmac'})
    res = ress.query('match', **{'cmsg':'xor'})


    # ress = ress.query('match', **{'centercode' :{'eq':''}})

    print(" System with XOR details : ", res.count())

    import time
    start_time = time.time()
    cmac_df = pd.DataFrame(d.to_dict() for d in res.scan())
    print("time for completion : ",  time.time() - start_time )
    if len(cmac_df) != 0:
    ##### ## commenting here. insert added last after the logic
    # cmac_records = json.loads(cmac_df.T.to_json()).values()
    # db.mac.insert_many(cmac_records)
    # print("time for completion : ",  time.time() - start_time )

        cmac_df = cmac_df[['centercode', 'cip', 'cmac', 'manufacturer', 'serialno', 'mod', 'mon', '@timestamp', 'cmsg']]
        cmac_df.rename(columns={'@timestamp' : 'timestamp'}, inplace=True)
        len(cmac_df)
        cmac_df.head(3)
        cmac_df.drop_duplicates(subset=cmac_df.columns.difference(['timestamp']), keep='last',  inplace=True)
        len(cmac_df)

            # cmac_df[(cmac_df.mon=='NA') | (cmac_df.manufacturer=='@@@') | (cmac_df.mod=='NA')]['centercode'].value_counts()
        # xor_nan_df = cmac_df[(cmac_df.mon=='NA') | (cmac_df.manufacturer=='@@@') | (cmac_df.mod=='NA')]
        xor_nan_df = cmac_df[(cmac_df.mon.str.contains('NA')) | (cmac_df.manufacturer.str.contains('@@@')) | (cmac_df['mod'].str.contains('NA')) 
                            ]
        #| (cmac_df.manufacturer.str.contains('ZZZ'))
        xor_nan_df.head(20)

        res_cfprint = ress.query('match', **{'cmsg':'cfprnt'})
        print(" System with CFPRINT details : ", res_cfprint.count())
        start_time = time.time()
        cfprint_df = pd.DataFrame(d.to_dict() for d in res_cfprint.scan())
        print("time for completion : ",  time.time() - start_time )

        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', None)
        # pd.set_option('display.expand_frame_repr', False)
        ##NA
        # cfprint_df[(cfprint_df.centercode =='419') & (cfprint_df.cmac=='0a:e0:af:b1:61:95')].message
        # cfprint_df[(cfprint_df.centercode =='350') & (cfprint_df.cmac=='00:19:99:5f:be:d4')].message

        ##NO NA
        # cfprint_df[(cfprint_df.centercode =='280') & (cfprint_df.cmac=='00:18:8b:61:be:9d')].message

        ## if no centercode for xor message
        len(xor_nan_df)
        xor_nan_df = xor_nan_df[~xor_nan_df.centercode.isna()]
        len(xor_nan_df)
        # cmac_df = cmac_df[~cmac_df.manufacturer.isna()]

        ##remove the leading and trailing white space
        xor_nan_df = xor_nan_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


        xor_nan_records = json.loads(xor_nan_df.T.to_json()).values()
        db.xor_nan.insert_many(xor_nan_records)
        db.matchxornan.insert_many(xor_nan_records)

        ress= Search(using = elastic_client,  index=alias )     


    # delyaed_login_time     = '2023-01-23T03:00:00.000Z'
    # delyaed_login_time_min = '2023-01-23T10:00:00.000Z'

        delyaed_login_time     = starttime
        delyaed_login_time_min = endtime

        # delyaed_login_time     = '2023-03-27T07:30:00.000Z'
        # delyaed_login_time_min = '2023-03-27T09:30:00.000Z'

        # delyaed_login_time     = '2023-03-27T11:00:00.000Z'
        # delyaed_login_time_min = '2023-03-27T12:30:00.000Z'

        examslot_var = slot
        

        ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min, }})
        ress.count()

        print("Login started................",examslot_var)        
        #     ress = ress.query('match', **{'examslot':examslot_var})
        ress = ress.query('match', **{'slot':examslot_var})
        # ress = ress.query('match', **{'papercode':'NAVIK-NG'})
        res = ress.query('match', **{'msg':configs.start_exam})        
        print("startExamination count " , res.count())
        res.count()

        login_df = pd.DataFrame(d.to_dict() for d in res.scan())

        if ( (len(login_df) >=1)  & (len(xor_nan_df) >=1 ) ):
            print("Both xor nan  and login data is avilable, need to related both ")
            login_df = login_df.loc[(login_df['msg'].notnull() & login_df['msg'].str.contains(configs.start_exam) )]        
            login_df.shape
            login_df = login_df.drop_duplicates(subset='candidateid', keep="last")
            login_df.head(2)

            login_df = login_df[[ "column8", "host", "fields", "centercode", "eventdate", "slot", "candidateid", "message",  ]]
            login_df = login_df.rename(columns={"column8": "cip"})
            xor_can_df = pd.merge(left=xor_nan_df, right=login_df,  how='inner',suffixes=('_xor', '_log'),
                                                on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("XOR NAN CANDIDATES : ", xor_can_df.shape)
            if(len(xor_can_df) >=1):
                xor_records_cand = json.loads(xor_can_df.T.to_json()).values()
                db.matchxornan.insert_many(xor_records_cand)
                print("XOR NAN CANDIDATES : DONE")
                print("time to complete function : ", time.time() - start_time)

                print("XOR NAN CONSOLIDATED BEGINS.............")
                consolidate_df = xor_can_df[['slot','centercode','candidateid','cip']]
                consolidate_df['status'] = 'xor'
                consolidate_df['status_desc'] = 'xor_nan'
                consolidate_df.columns = ['examslot','centercode','candidateid', 'cip','status' , 'status_desc']        
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE XOR NAN : DONE")
                print("time to complete function : ", time.time() - start_time)
                error_message = "xornan Successfully Completed"
                views.statuslog(un,starttime,endtime,slot,error_message, 'xornan')

            else:
                print("NO candidates in XOR NAN system")
                error_message = "NO candidates in XOR NAN system"
                views.statuslog(un,starttime,endtime,slot,error_message, 'xornan')
        else:
            print("xor alert OR login data is NOT avilable - so no need to relate further.")
    else:
        print("NO XORNAN CANDIDATES")
        error_message = "NO XORNAN CANDIDATES"
        views.statuslog(un,starttime,endtime,slot,error_message, 'xornan')