import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
##!jupyter nbconvert <notebook name>.ipynb --to html
from datetime import datetime

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

def csmgfn(starttime, endtime, qstarttime,slot,un,index):
##changes ##cell added
# def quickFinisher():
    print('CSMG')
    details = views.getdetails(un)
    
    print("=-=--=-=",details)
    
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
        print("=-=-=-=-=-=-",alias)
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'CSMG')
    
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]

    global delyaed_min_login_time
# def startExamDelay():
    
    delyaed_min_login_time = qstarttime
#     delyaed_min_login_time = '2022-07-31T05:30:00.000Z'
#     delyaed_min_login_time = '2022-07-31T07:30:00.000Z'
#     delyaed_min_login_time =  2022-07-31T10:00:00.000Z'
    examslot_var = slot


    
    ### First time it will start from user given TIME, if empty current time will be taken
    if delyaed_min_login_time == '':
        now = datetime.now()
        delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
    else:
        delyaed_login_time = delyaed_min_login_time

    print("delyaed_login_time : ", delyaed_login_time)    
    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias ) 
    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    print("RESS: ", ress.count())
#     ress = ress.query('match', **{'papercode':'NAVIK-NG'})

#     ress = ress.query('match', **{'examslot':examslot_var})
    res = ress.query('match', **{'cmsg':'AMT'})
    print(res.count())
    
#     res = res.query('match', **{'centercode':'123'})
    # res = res.query('match', **{'examslot':'C'})
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
    amt_df = pd.DataFrame()
    if len(results_df)>=1:
        
        print("time for completion : ",  time.time() - start_time )
        print( "AMT length is", len( results_df['centercode'].unique() ))
        print( " Data frame "  )
    #     results_df.head(2)
    #     if(len(results_df) != 0):

        amt_df = results_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        amt_df = amt_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
        amt_records = json.loads(amt_df.T.to_json()).values()
        db.amt.insert_many(amt_records)
        print("AMT : DONE")
        print("time to complete function : ", time.time() - start_time)
    else:
        print("No AMT found")
        
###########################################
    res = ress.query('match', **{'cmsg':'RMF'})
    cnt
    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)
    import time
    start_time = time.time()
    rmf_df = pd.DataFrame(d.to_dict() for d in res.scan())
    print("shape:: ", rmf_df.shape)
    if (len(rmf_df)):
        print("time for completion : ",  time.time() - start_time )
        print( "RMF length is", len( rmf_df['centercode'].unique() ))
#          = rmf_df.groupby(['centercode', 'cip', 'sessionid']).first().reset_index()

        print( " Data frame "  )
#         rmf_df.head(2)
    #     if(len(results_df) != 0):
        rmf_df = rmf_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        rmf_df = rmf_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
    
        rmf_records = json.loads(rmf_df.T.to_json()).values()
        db.rmf.insert_many(rmf_records)
        print("RMF : DONE")
        print("time to complete function : ", time.time() - start_time)
    else:
        print("No RMF found")
    
##############################################    
    res = ress.query('match', **{'cmsg':'MM'})
    cnt
    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)
    import time
    start_time = time.time()
    mm_df = pd.DataFrame(d.to_dict() for d in res.scan())
    
    if (len(mm_df)):
        print("time for completion : ",  time.time() - start_time )
        print( "MM length is", len( mm_df['centercode'].unique() ))
        print( " Data frame "  )
#         rmf_df.head(2)
    #     if(len(results_df) != 0):
        mm_df = mm_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        mm_df = mm_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
    
        mm_records = json.loads(mm_df.T.to_json()).values()
        db.mm.insert_many(mm_records)
        print("MM : DONE")
        print("time to complete function : ", time.time() - start_time)
    else:
        print("No MM found")
        
##############################################           
    res = ress.query('match', **{'cmsg':'MS'})
    cnt
    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)
    import time
    start_time = time.time()
    ms_df = pd.DataFrame(d.to_dict() for d in res.scan())
    
    if (len(ms_df)):
        print("time for completion : ",  time.time() - start_time )
        print( "MS length is", len( ms_df['centercode'].unique() ))
        print( " Data frame "  )
#         rmf_df.head(2)
    #     if(len(results_df) != 0):
        ms_df = ms_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        ms_df = ms_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
    
        ms_records = json.loads(ms_df.T.to_json()).values()
        db.ms.insert_many(ms_records)
        print("MS : DONE")
        print("time to complete MS function : ", time.time() - start_time)
    else:
        print("No MS found")

    ##############################################
    ### MULTIPLE SEARCH BASED ON OR CONDITION ####
    ##############################################

    from elasticsearch_dsl import Q
    q = Q("match", cmsg='MN') | Q("match", cmsg='MNA') | Q("match", cmsg='MK') | Q("match", cmsg='MF')   
    print("Inside Q")
    res = ress.query(q) 
    import time
    start_time = time.time()
    nkm_df = pd.DataFrame(d.to_dict() for d in res.scan())
    mn_df = pd.DataFrame()
    mna_df = pd.DataFrame()
    mk_df = pd.DataFrame()
    
    mf_df = pd.DataFrame()
    if (len(nkm_df)):
        print("time for completion : ",  time.time() - start_time )
        print( "NKM length is", len( nkm_df['centercode'].unique() ))
        print( " Data frame "  )
#         rmf_df.head(2)
    #     if(len(results_df) != 0):
        nkm_df = nkm_df[["centercode","cip", "host", "cmsg", "cmac", "cint", "fields", "column1", "message"]]
        nkm_df = nkm_df.drop_duplicates(['cip','centercode','cmac'], keep='last')
        if nkm_df[nkm_df.cmsg=='MN'].shape[0] > 1:
            mn_df = nkm_df[nkm_df.cmsg=='MN']
            mn_records = json.loads(mn_df.T.to_json()).values()        
            db.mn.insert_many(mn_records)
            print("MN : DONE")
            print("time to complete MN function : ", time.time() - start_time)
        else:
            print("No MN found")
        if nkm_df[nkm_df.cmsg=='MNA'].shape[0] > 1:
            mna_df = nkm_df[nkm_df.cmsg=='MNA']
            mna_records = json.loads(mna_df.T.to_json()).values()        
            db.mna.insert_many(mna_records)
            print("MNA : DONE")
            print("time to complete MNA function : ", time.time() - start_time)
        else:
            print("No MNA found")
            
        if nkm_df[nkm_df.cmsg=='MK'].shape[0] > 1:
            mk_df = nkm_df[nkm_df.cmsg=='MK']
            mk_records = json.loads(mk_df.T.to_json()).values()        
            db.mk.insert_many(mk_records)
            print("MK : DONE")
            print("time to complete MK function : ", time.time() - start_time)
        else:
            print("No MK found")
            
        if nkm_df[nkm_df.cmsg=='MF'].shape[0] > 1:
            print("MF")
            mf_df = nkm_df[nkm_df.cmsg=='MF']
            mf_records = json.loads(mf_df.T.to_json()).values()        
            db.mf.insert_many(mf_records)
            print("MF : DONE")
            print("time to complete MF function : ", time.time() - start_time)
        else:
            print("No MF found")
           
        
            ###########################################    
                    #### Login started ####
            ###########################################    
    print("Login started................ ", examslot_var)        
    ress = ress.query('match', **{'examslot':examslot_var})
#     ress = ress.query('match', **{'slot':examslot_var})
#     ress = ress.query('match', **{'papercode':'NAVIK-NG'})

    res = ress.query('match', **{'msg':configs.start_exam})        
    print("STart exm , ", res.count())
        # rmf_df
    if (((len(results_df) >=1)  | (len(rmf_df) >=1 )  | (len(mm_df) >=1 ) ) & (res.count()>0 )):
      
        print("Login started................")        
#         ress = ress.query('match', **{'examslot':examslot_var})
#         res = ress.query('match', **{'msg':'startExamination'})        

#         res = ress.query('match', **{'msg':'submitAnswer'})
#         res = res.query('match', **{'examslot':'U'})
#         res = ress.query('match', **{'msg':'submitAnswer'})
        print("startExamination count " , res.count())
        res.count()
        
        
        
        login_df = pd.DataFrame(d.to_dict() for d in res.scan())
#         login_df.head()
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
        if len(amt_df)>1:
            amt_can_df = pd.merge(left=amt_df, right=login_df,  how='inner',suffixes=('_amt', '_log'),
                                          on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("AMT CANDIDATES : ", amt_can_df.shape)
            if(len(amt_can_df) >=1):
                amt_records_cand = json.loads(amt_can_df.T.to_json()).values()
                db.matchamt.insert_many(amt_records_cand)
                print("AMT CANDIDATES : DONE")
                print("time to complete function : ", time.time() - start_time)

                print("AMT CONSOLIDATED BEGINS.............")
                consolidate_df = amt_can_df[['slot','centercode','candidateid','cip']]
                consolidate_df['status'] = 'amt'
                consolidate_df.columns = ['examslot','centercode','candidateid', 'cip','status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE AMT : DONE")
                print("time to complete function : ", time.time() - start_time)    

            else:
                print("NO candidates in AMT system")

        print(rmf_df.head(2))
        rmf_can_df = pd.merge(left=rmf_df, right=login_df,  how='inner',suffixes=('_rmf', '_log'),
                                          on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
        print("RMF CANDIDATES : ", rmf_can_df.shape)

        if len(rmf_can_df) >=1:
            rmf_records_cand = json.loads(rmf_can_df.T.to_json()).values()
            db.matchrmf.insert_many(rmf_records_cand)
            print("RMF CANDIDATES : DONE")
            print("time to complete function : ", time.time() - start_time)

            print("RMF CONSOLIDATED BEGINS.............")
            consolidate_df = rmf_can_df[['slot','centercode','candidateid', 'cip']]
            consolidate_df['status'] = 'rmf'
            consolidate_df.columns = ['examslot','centercode','candidateid', 'cip','status']
            records = json.loads(consolidate_df.T.to_json()).values()
            db.consolidate.insert_many(records)
            print("CONSOLIDATE RMF : DONE")
            print("time to complete function : ", time.time() - start_time)    
        else:
            print("No Candidates in RMF system")
            
            
        if len(mm_df) > 1:
            mm_can_df = pd.merge(left=mm_df, right=login_df,  how='inner',suffixes=('_mm', '_log'),
                                          on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
        
            print("MM CANDIDATES : ", mm_can_df.shape)

            if len(mm_can_df) >=1:
                mm_records_cand = json.loads(mm_can_df.T.to_json()).values()
                db.matchmm.insert_many(mm_records_cand)
                print("MM CANDIDATES : DONE")
                print("time to complete function : ", time.time() - start_time)

                print("MM CONSOLIDATED BEGINS.............")
                consolidate_df = mm_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'mm'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE MM : DONE")
                print("time to complete function : ", time.time() - start_time)    
            else:
                print("No Candidates in MM system")
        else:
            print("NO MM")

##################### MS login info
        if len(ms_df) >=1:
            ms_can_df = pd.merge(left=ms_df, right=login_df,  how='inner',suffixes=('_ms', '_log'),on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("MS CANDIDATES : ", ms_can_df.shape)

            if len(ms_can_df) >=1:
                ms_records_cand = json.loads(ms_can_df.T.to_json()).values()
                db.matchms.insert_many(ms_records_cand)
                print("MS CANDIDATES : DONE")
                print("time to complete MS and LOGIN function : ", time.time() - start_time)

                print("MS CONSOLIDATED BEGINS.............")
                consolidate_df = ms_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'ms'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE MS : DONE")
                print("time to complete MS and Login 2 function : ", time.time() - start_time)    
            else:
                print("No Candidates in MS system")
        else:
            print("NO MS")
            
##################### MN login info    
        if len(mn_df) >=1:
            
            mn_can_df = pd.merge(left=mn_df, right=login_df,  how='inner',suffixes=('_mn', '_log'),
                                              on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("MN CANDIDATES : ", mn_can_df.shape)

            if len(mn_can_df) >=1:
                mn_records_cand = json.loads(mn_can_df.T.to_json()).values()
                db.matchmn.insert_many(mn_records_cand)
                print("MN CANDIDATES : DONE")
                print("time to complete MN and LOGIN function : ", time.time() - start_time)

                print("MN CONSOLIDATED BEGINS.............")
                consolidate_df = mn_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'mn'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE MN : DONE")
                print("time to complete MN and Login 2 function : ", time.time() - start_time)    
            else:
                print("No Candidates in MN system")
        else:
            print("NO MN")
    
    
#####################  mna login info   
        if len(mna_df) >=1:
            mna_can_df = pd.merge(left= mna_df, right=login_df,  how='inner',suffixes=('_ mna', '_log'),
                                              on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print(" mna CANDIDATES : ",  mna_can_df.shape)

            if len( mna_can_df) >=1:
                mna_records_cand = json.loads( mna_can_df.T.to_json()).values()
                db.matchmna.insert_many( mna_records_cand)
                print(" mna CANDIDATES : DONE")
                print("time to complete  mna and LOGIN function : ", time.time() - start_time)

                print(" mna CONSOLIDATED BEGINS.............")
                consolidate_df =  mna_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'mna'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE  mna : DONE")
                print("time to complete  mna and Login 2 function : ", time.time() - start_time)    
            else:
                print("No Candidates in  mna system")
        else:
            print("NO MNA")
            
            
#####################   mk login info    
        if len(mk_df) >=1:

            mk_can_df = pd.merge(left=  mk_df, right=login_df,  how='inner',suffixes=('_  mk', '_log'),
                                              on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("  mk CANDIDATES : ",   mk_can_df.shape)

            if len(  mk_can_df) >=1:
                mk_records_cand = json.loads(  mk_can_df.T.to_json()).values()
                db.matchmk.insert_many(  mk_records_cand)
                print("  mk CANDIDATES : DONE")
                print("time to complete   mk and LOGIN function : ", time.time() - start_time)

                print("  mk CONSOLIDATED BEGINS.............")
                consolidate_df =   mk_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'mk'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE   mk : DONE")
                print("time to complete   mk and Login 2 function : ", time.time() - start_time)    
            else:
                print("No Candidates in   mk system")
        else:
            print("NO MK")
            

#####################   mf login info     
        if len(ms_df) >=1:

            mf_can_df = pd.merge(left=  mf_df, right=login_df,  how='inner',suffixes=('_  mf', '_log'),
                                              on= ['cip','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
            print("  mf CANDIDATES : ",   mf_can_df.shape)

            if len(  mf_can_df) >=1:
                mf_records_cand = json.loads(  mf_can_df.T.to_json()).values()
                db.matchmf.insert_many(  mf_records_cand)
                print("  mf CANDIDATES : DONE")
                print("time to complete   mf and LOGIN function : ", time.time() - start_time)

                print("  mf CONSOLIDATED BEGINS.............")
                consolidate_df =   mf_can_df[['slot','centercode','candidateid', 'cip']]
                consolidate_df['status'] = 'mf'
#                 consolidate_df['status'] = 'ms'
                consolidate_df.columns = ['examslot','centercode','candidateid','cip', 'status']
                records = json.loads(consolidate_df.T.to_json()).values()
                db.consolidate.insert_many(records)
                print("CONSOLIDATE   mf : DONE")
                print("time to complete   mf and Login 2 function : ", time.time() - start_time)    
            else:
                print("No Candidates in   mf system")  
        else:
            print("NO MF")
     #### SUCCESS MESSAGES
            error_message = "CSMG SUCCESSFUL"
            views.statuslog(un,starttime,endtime,slot,error_message, 'CSMG')
    else:
        print("Login not executed")
        error_message = "LOGIN NOT EXECUTED"
        views.statuslog(un,starttime,endtime,slot,error_message, 'CSMG')

 ### STOP HERE