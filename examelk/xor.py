import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
##!jupyter nbconvert <notebook name>.ipynb --to html

#!pip3 install pandas==0.20.3
import pandas as pd
from urllib.parse import quote
from .models import *
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

def xorfn(starttime, endtime, qstarttime,qendtime,slot,un,index):
##changes ##cell added
# def quickFinisher():

    details = views.getdetails(un)
    
    print("=-=--=-=",details)
    
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
        print("=-=-=-=-=-=-",alias)
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'XOR')
    
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]

    global delyaed_min_login_time

    ############## based on the time , extract all the document having cmac field in it.
    ress= Search(using = elastic_client,  index=alias )     

    delyaed_login_time     = qstarttime
    # delyaed_login_time_min = '2023-01-23T10:30:00.000Z'

    # delyaed_login_time     = '2023-01-23T07:30:00.000Z'
    delyaed_login_time_min = qendtime



    # delyaed_login_time     = '2022-07-31T00:30:00.000Z'
    # delyaed_login_time_min = '2022-07-31T05:30:00.000Z'

    # delyaed_login_time     = '2022-07-31T04:30:00.000Z'
    # delyaed_login_time_min = '2022-07-30T17:00:00.000Z'

    # delyaed_login_time     = '2022-07-31T05:30:00.000Z'
    # delyaed_login_time_min = '2022-07-31T11:00:00.000Z'

    # examslot_var = 'B'

    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min, }})


    ress.count()
    # ress = ress.query('exists',**{'field' :'cmac'})
    ress = ress.query('match', **{'cmsg':'xor'})


    # ress = ress.query('match', **{'centercode' :{'eq':''}})

    print(" System with XOR details : ", ress.count())

    import time
    start_time = time.time()
    cmac_df = pd.DataFrame(d.to_dict() for d in ress.scan())
    print("time for completion : ",  time.time() - start_time )
    if len(cmac_df) != 0:
    ##### ## commenting here. insert added last after the logic
    # cmac_records = json.loads(cmac_df.T.to_json()).values()
    # db.mac.insert_many(cmac_records)
    # print("time for completion : ",  time.time() - start_time )

        cmac_df = cmac_df[['centercode', 'cip', 'cmac', 'manufacturer', 'serialno', 'mod', 'mon', '@timestamp', 'cmsg']]
        cmac_df.rename(columns={'@timestamp' : 'timestamp'}, inplace=True)
        cmac_df.head(1)
        cmac_df.drop_duplicates(subset=cmac_df.columns.difference(['timestamp']), keep='last',  inplace=True)

        ## if no centercode for xor message
        len(cmac_df)
        cmac_df = cmac_df[~cmac_df.centercode.isna()]
        len(cmac_df)
    # cmac_df = cmac_df[~cmac_df.manufacturer.isna()]

        kvm_df = pd.DataFrame(list(db.edid_kvm.find({}, {'_id':0})))
        kvm_df.head(2)
        kvm_df.shape

        if kvm_df.shape[0] !=0:
            cmac_df = cmac_df.merge(kvm_df, left_on='mod', right_on='model', how='left')[["centercode","cip","cmac","manufacturer_x","serialno","mod","mon","timestamp","cmsg", "kvm"]]
            cmac_df.rename(columns = {"manufacturer_x":"manufacturer"}, inplace=True)
            cmac_df.fillna(value={'kvm':'no'}, inplace=True)
            cmac_df.head()
        
        cmac_df[cmac_df.kvm=='yes']

        ##remove the leading and trailing white space
        cmac_df = cmac_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        #cmac_df.manufacturer[0:5].apply(len)

        mac_new_records = json.loads(cmac_df.T.to_json()).values()
        db.xor_hist.insert_many(mac_new_records)

        import numpy as np
        stime = time.time()

        # xor_duplicate = pd.DataFrame(list(db.xor_hist.aggregate([{'$match':{'centercode':{'$ne':''}}},  {'$group':{'_id':{'centercode':'$centercode', 'cmac':'$cmac' , 'manufacturer':'$manufacturer', 'mod':'$mod', 'mon':'$mon','cip':'$cip' }, 'count':{'$sum':1}}}, {'$project':{'centercode':"$_id.centercode", 'cmac':"$_id.cmac", 'manufacturer':'$_id.manufacturer', 'mod':"$_id.mod", 'mon':"$_id.mon",'cip':"$_id.cip", 'count':'$count', '_id':0}}, {'$sort':{'centercode':1, 'cmac':1}}, {"$group":{"_id":{"centercode":"$centercode", "cmac":"$cmac"},"count":{"$sum":1}, "data": { "$push": "$$ROOT" }}}, {"$match":{"count":{"$gt":1}}}, {"$unwind": "$data"}, {'$project':{'centercode':"$data.centercode",'cmac':"$data.cmac", 'manufacturer':"$data.manufacturer", 'mod':"$data.mod", 'mon':"$data.mon", 'cip':"$data.cip" ,'_id':0}} ])))
        xor_duplicate = pd.DataFrame(list(db.xor_hist.aggregate([{'$match':{'centercode':{'$ne':''}}},  {'$group':{'_id':{'centercode':'$centercode', 'cmac':'$cmac' , 'manufacturer':'$manufacturer', 'mod':'$mod', 'mon':'$mon' }, 'count':{'$sum':1}}}, {'$project':{'centercode':"$_id.centercode", 'cmac':"$_id.cmac", 'manufacturer':'$_id.manufacturer', 'mod':"$_id.mod", 'mon':"$_id.mon", 'count':'$count', '_id':0}}, {'$sort':{'centercode':1, 'cmac':1}}, {"$group":{"_id":{"centercode":"$centercode", "cmac":"$cmac"},"count":{"$sum":1}, "data": { "$push": "$$ROOT" }}}, {"$match":{"count":{"$gt":1}}}, {"$unwind": "$data"}, {'$project':{'centercode':"$data.centercode",'cmac':"$data.cmac", 'manufacturer':"$data.manufacturer", 'mod':"$data.mod", 'mon':"$data.mon" ,'_id':0}} ])))
        xor_duplicate.head()

        print("time to complete function : ", time.time() - stime)

        xor_duplicate.shape
        # xor_duplicate.drop_duplicates(subset=['centercode', 'cmac','manufacturer', 'mod', 'mon', 'cip'], keep=False, inplace=True)
        xor_duplicate.drop_duplicates(subset=['centercode', 'cmac','manufacturer', 'mod', 'mon'], keep=False, inplace=True)
        xor_duplicate.shape

        lst_cmac = xor_duplicate.sort_values(['centercode', 'cmac']).cmac.tolist()


        duplicate_edid_df = pd.DataFrame()

        print("time to complete function : ", time.time() - stime)

        for i in lst_cmac:  
        #     print(i)
        #     # convert table to df    
            table = pd.DataFrame(list(db.xor_hist.find({'cmac':i } )))
            duplicate_edid_df = pd.concat([duplicate_edid_df, table])

        print("time to complete function for loop : ", time.time() - stime)

        duplicate_edid_df = duplicate_edid_df.drop_duplicates(subset=['centercode', 'cmac', 'manufacturer', 'mod', 'mon'], keep='last')


        #--- SHIFT insdead of JOIN
        duplicate_edid_df_test = duplicate_edid_df.sort_values(['centercode','cmac', 'timestamp'])
        duplicate_edid_df_test.head()
        duplicate_edid_df_test['new_manufacturer'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['manufacturer'].shift(-1)
        duplicate_edid_df_test['new_mod'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['mod'].shift(-1)
        duplicate_edid_df_test['new_mon'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['mon'].shift(-1)
        duplicate_edid_df_test['new_timestamp'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['timestamp'].shift(-1)
        duplicate_edid_df_test['new_cip'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['cip'].shift(-1)
        duplicate_edid_df_test['new_serialno'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['serialno'].shift(-1)
        duplicate_edid_df_test['_id_y'] = duplicate_edid_df_test.groupby(['cmac', 'centercode'])['_id'].shift(-1)

        print("time to complete function SHIFT : ", time.time() - stime)

        duplicate_edid_df_test = duplicate_edid_df_test[~duplicate_edid_df_test['new_manufacturer'].isna()]
        duplicate_edid_df_test.sort_values(['new_timestamp'], ascending=False)[0:5]



        duplicate_edid_df_test.rename(columns={'_id':'_id_x', 'cip':'old_cip', 'manufacturer':'old_manufacturer','serialno':'old_serialno','mod':'old_mod','mon':'old_mon','timestamp':'old_timestamp'}, inplace=True)


        duplicate_edid_df_join = duplicate_edid_df_test
        duplicate_edid_df_join['is_alert'] = 'alert'
        # duplicate_edid_df_join.head(1)
        ### duplicate_edid_df_join = duplicate_edid_df_join[[ '_id_x', '_id_y','centercode', 'cmac', 'cip_x', 'manufacturer_x','serialno_x','mod_x','mon_x','timestamp_x','manufacturer_y','serialno_y','mod_y','mon_y','timestamp_y', 'is_alert', 'cip_y']]
        ####duplicate_edid_df_join.columns = ['_id_x', '_id_y','centercode', 'cmac', 'old_cip', 'old_manufacturer','old_serialno','old_mod','old_mon','old_timestamp','new_manufacturer','new_serialno','new_mod','new_mon','new_timestamp', 'is_alert', 'new_cip']
        duplicate_edid_df_join.head(2)


        duplicate_edid_df_join.shape
        duplicate_edid_df_join['remove'] = np.where( (duplicate_edid_df_join['_id_x']!=duplicate_edid_df_join['_id_y'] )  , 'alert', 'normal') 
        duplicate_edid_df_join = duplicate_edid_df_join[duplicate_edid_df_join.remove == 'alert']
        duplicate_edid_df_join.shape



        duplicate_edid_df_join = duplicate_edid_df_join[['_id_x', '_id_y','centercode', 'cmac', 'old_cip', 'old_manufacturer','old_serialno','old_mod','old_mon','old_timestamp','new_manufacturer','new_serialno','new_mod','new_mon','new_timestamp', 'is_alert', 'new_cip']]
        duplicate_edid_df_join.reset_index(inplace=True)
        duplicate_edid_df_join_records = json.loads(duplicate_edid_df_join.T.to_json(default_handler=str)).values()  
        db.matchxor.insert_many(duplicate_edid_df_join_records)
        print("time to complete function FINAL: ", time.time() - stime)

        ress= Search(using = elastic_client,  index=alias )     

        # delyaed_login_time     = '2022-07-31T03:00:00.000Z'
        # delyaed_login_time_min = '2022-07-31T05:00:00.000Z'

        # delyaed_login_time     = '2022-07-31T07:30:00.000Z'
        # delyaed_login_time_min = '2022-07-31T09:00:00.000Z'

        # delyaed_login_time     = '2023-01-23T04:30:00.000Z'
        # delyaed_login_time_min = '2023-01-23T05:50:00.000Z'

        delyaed_login_time     = starttime
        delyaed_login_time_min = endtime

        # delyaed_login_time     = '2023-03-27T07:30:00.000Z'
        # delyaed_login_time_min = '2023-03-27T09:00:00.000Z'

        # delyaed_login_time     = '2023-03-27T11:00:00.000Z'
        # delyaed_login_time_min = '2023-03-27T13:00:00.000Z'




        examslot_var = slot

        ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min, }})
        ress.count()

        print("Login started................")        
        #     ress = ress.query('match', **{'examslot':examslot_var})
        ress = ress.query('match', **{'slot':examslot_var})
        # ress = ress.query('match', **{'papercode':'NAVIK-NG'})
        res = ress.query('match', **{'msg':configs.start_exam})     

        print("startExamination count " , res.count())
        res.count()

        login_df = pd.DataFrame(d.to_dict() for d in res.scan())

        if ( (len(login_df) >=1)  & (len(duplicate_edid_df_join) >=1 ) ):
            print("Both xor alert and login data is avilable, need to related both ")
            login_df = login_df.loc[(login_df['msg'].notnull() & login_df['msg'].str.contains(configs.start_exam) )]        
            login_df.shape
            login_df = login_df.drop_duplicates(subset='candidateid', keep="last")
            login_df.head(2)

            login_df = login_df[[ "column8", "host", "fields", "centercode", "eventdate", "slot", "candidateid", "message",  ]]
            login_df = login_df.rename(columns={"column8": "cip"})
            xor_can_df = pd.merge(left=duplicate_edid_df_join, right=login_df,  how='inner',suffixes=('_xor', '_log'),
                                                #on= ['cip','centercode']
                                left_on=['new_cip','centercode'], right_on = ['cip','centercode'] )
            print("XOR CANDIDATES : ", xor_can_df.shape)
            if(len(xor_can_df) >=1):
                xor_records_cand = json.loads(xor_can_df.T.to_json(default_handler=str)).values()
        #         db.matchxor.insert_many(xor_records_cand)
                db.matchxor_cand.insert_many(xor_records_cand)
                print("XOR CANDIDATES : DONE")
                print("time to complete function : ", time.time() - start_time)

                print("XOR CONSOLIDATED BEGINS.............")
                consolidate_df = xor_can_df[['slot','centercode','candidateid','cip']]
                consolidate_df['status'] = 'xor'
                consolidate_df.columns = ['examslot','centercode','candidateid', 'cip','status']        
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE XOR : DONE")
                print("time to complete function : ", time.time() - start_time)
                error_message = "   XOR Successfully Completed"
                views.statuslog(un,starttime,endtime,slot,error_message, 'xor')

            else:
                print("NO candidates in XOR system") 
                error_message = "NO candidates in XOR system"
                views.statuslog(un,starttime,endtime,slot,error_message, 'XOR')                     
        else:
            print("xor alert OR login data is NOT avilable - so no need to relate further.")
    else:
        print("NO SYSTEM WITH XOR")
        error_message = "NO SYSTEN WITH XOR"
        views.statuslog(un,starttime,endtime,slot,error_message, 'xor')  