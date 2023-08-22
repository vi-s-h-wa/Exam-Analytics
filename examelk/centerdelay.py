#from IPython.core.interactiveshell import InteractiveShell
#InteractiveShell.ast_node_interactivity = "all"
#import importlib
#import config
#importlib.reload(config)
import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
##!jupyter nbconvert <notebook name>.ipynb --to html
from .models import *
#!pip3 install pandas==0.20.3
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from examelk import views, configs

import pymongo
import json
from pymongo import MongoClient
import time

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 51

#import importlib
#import config
#importlib.reload(config)

###sudo update-ca-certificates
ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/emsclient.crt','client_key':'/usr/local/share/ca-certificates/elastic/client.key'}

def centerdelayfn(starttime,endtime,slot,unaware,actual,un,index):
    print('centerdelay')
    print(starttime,slot,'unaware',type(unaware),'actual',actual,index)
    details = views.getdetails(un)
    elastic_client = Elasticsearch(host=details['elkip'], port=details['elkport'], verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=(details['elkuname'],details['elkpwd']),timeout=300)
    alias = []
    try:
        alias.extend(elastic_client.indices.get_alias(index))
        alias
    except:
        error_message = "Invalid Index"
        views.statuslog(un,starttime,endtime,slot,error_message, 'CenterDelay')
    from urllib.parse import quote
################ MONGODB conaction details
# mongod --bind_ip 10.184.51.194 --dbpath /var/lib/mongodb/
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]

    # actualExamTime = 'November 4, 2020, 09:00:00 AM'
    from datetime import datetime

    ##ACTUAL EXAM START TIME
    ## yaer, month, date, hour, min, sec, timezone

    # unaware = datetime(2023, 2, 26, 10, 0, 0, 0)
    # unaware = datetime(2023, 3, 22, 15, 0, 0, 0)
    # unaware = datetime(2023, 1, 16, 10, 15, 0, 0)

    unaware = unaware
    # unaware = datetime(2023, 5, 1, 9, 45, 0, 0)

    # unaware = datetime(2023, 3, 27, 13, 0, 0, 0)
    # unaware = datetime(2023, 3, 27, 16, 45, 0, 0)
    
    actualExamTime = actual
    # actualExamTime = 'May 01, 2023, 09:45:00 AM'
    # actualExamTime = 'March 27, 2023, 03:00:00 PM'
    # actualExamTime = 'March 27, 2023, 04:45:00 PM'


    ######################### EXAM TIME for EACH SLOT
    import pytz
    from datetime import datetime
    import time
    from pytz import timezone

    stime = time.time()
        


    ## yaer, month, date, hour, min, sec, timezone
    # unaware = datetime(2020, 11, 4, 10, 30, 0, 0)
    # exam_time = pytz.utc.localize(unaware)
    # exam_time
    localtz = timezone('Asia/Kolkata')
    exam_time = localtz.localize(unaware)
    print(exam_time)

    ######################### DATA LOG CRAWLING DATE & TIME
    # delyaed_min_login_time = '2022-07-31T03:30:00.000Z'
    # delyaed_min_login_time = '2022-07-31T07:20:00.000Z'

    # delyaed_min_login_time = '2023-01-23T04:30:00.000Z'
    # delyaed_min_login_time = '2023-02-26T04:30:00.000Z'

    delyaed_min_login_time = starttime
    # delyaed_min_login_time = '2023-03-28T07:30:00.000Z'
    # delyaed_min_login_time = '2023-03-27T11:00:00.000Z'
    # 
    examSlotVar = slot

    ### First time it will start from user given TIME, if empty current time will be taken
    if delyaed_min_login_time == '':
        now = datetime.now()
        delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
    else:
        delyaed_login_time = delyaed_min_login_time
        

    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias )
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    # ress = ress.query('range', **{'eventdate': {'gte': '2020-03-09T00:00:00.000Z'}})
    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    print("ress.count()  ", ress.count())
    ##change 1 #added
    # res = ress.query('match', **{'msg':'SUCCESS: Make Paper Section Available'})
    res = ress.query('match', **{'msg':configs.make_paper})
    print("MAKE ", res.count())
    print("delyaed_login_time ", delyaed_login_time)
    #     res = ress.query('match', **{'makePaperSectionAvailable'})
    # res = res.filter('terms', examslot=['L', 'B', 'E'])

    res = res.query('match', **{'examslot':examSlotVar})
    # res = res.query('match', **{'papercode':'XX'})



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
    results_df.shape
    print("time for completion : ",  time.time() - start_time )
    # len( results_df['centercode'].unique() )
    print(results_df.head(1))
    if len(results_df) != 0:
    #### set Minimum Time to crawl NEXT time
        delyaed_min_login_time = results_df['eventdate'].min()


        mpa_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains(configs.make_paper) ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]
        mpa_df = mpa_df[['msg','centercode','eventdate', 'papercode', 'examslot']]


        print("MPA_DF ", mpa_df)
        # results_df.head(1)
        ############## Data Cleansing and Transformation.
        # mpa_df['papercode'] = results_df.column4.str[-3:-1]
        print(mpa_df.head(1))
        #     mpa_df['column7'] = results_df.column7.str[:-1]



        ############EACH CENTER has multiple make paper available for SAME paper/slot
        ## SO Ihave grouped and takem=n MIN time-- may be its test data not in actual I GUESS
        ## so that we can know its started on time or not. 
        idx = mpa_df.groupby(['centercode', 'papercode','examslot'])['eventdate'].transform(min) == mpa_df['eventdate']
        mpa_df = mpa_df[idx]
        print("MPA : ")
        # print(mpa_df.head(1))


        ## Convert UST to IST
        mpa_df['eventdate_d'] = pd.to_datetime(mpa_df.eventdate)
        import datetime
        from pytz import timezone
        isttime = []
        for date in mpa_df['eventdate_d']:
        #     print(date.astimezone(timezone('Asia/Kolkata')))
            isttime.append(date.astimezone(timezone('Asia/Kolkata')))
            
        mpa_df['eventdate_ist'] = isttime
        mpa_df.head(4)

        #mpa_df['centre_delay'] = mpa_df['eventdate_ist'].astype('timedelta64[m]')
        #mpa_df['centre_delay']=mpa_df['eventdate_ist'].rsub( exam_time).astype('timedelta64[m]')
        #mpa_df['centre_delay'] = (mpa_df['eventdate_ist'] - exam_time).astype('timedelta64[m]')
        mpa_df['eventdate_ist'] = pd.to_datetime(mpa_df['eventdate_ist'])
        #mpa_df['eventdate_cend'] = pd.to_datetime(mpa_df['eventdate_cend'])

            # Calculate time difference in minutes
        time_diff_ns = (mpa_df['eventdate_ist'] - exam_time).dt.total_seconds() * 1e9
        minute_td = pd.to_timedelta(1, unit='m')
        
        mpa_df['centre_delay'] = pd.to_timedelta(time_diff_ns).divide(minute_td)
            # data['Aging'] = data['Created_Date'].rsub(today, axis=0).dt.days
        mpa_df['centre_delay']= [-i for i in mpa_df['centre_delay']]

        mpa_df.loc[ (mpa_df.centre_delay >= -3) & (mpa_df.centre_delay <=3 ),  'centre_delay_c' ] = "ontime"

        mpa_df.loc[ (mpa_df.centre_delay <= -4 ),  'centre_delay_c' ] = "early"
        mpa_df.loc[ (mpa_df.centre_delay >= 4) & (mpa_df.centre_delay <=10 ),  'centre_delay_c' ] = "slow"
        mpa_df.loc[ (mpa_df.centre_delay >= 11),  'centre_delay_c' ] = "delayed"


        mpa_df.eventdate_ist =  mpa_df.eventdate_ist.astype(str)
        mpa_df.eventdate_d = mpa_df.eventdate_d.astype(str)
        mpa_df['actual_exam_time']=actualExamTime
        mpa_df[['centercode', 'eventdate_ist', 'centre_delay', 'centre_delay_c']].head(20)

        


        # # sexm_tdiff_df


        # ########### inserting into MongoDB database
        # # record1 = db['qpattern']
        # ##drop
        # db.centerdelay.drop()
        # ##Insert
        records = json.loads(mpa_df.T.to_json()).values()
        db.centerdelay.insert_many(records)
        print("CENTRE DELAY : DONE")
        print("time to complete function : ", time.time() - stime)
        error_message = "CenterDelay Successfully Completed"
        views.statuslog(un,starttime,endtime,slot,error_message, 'CenterDelay')

    else:
        print("NO - PAPER MAKE AVAILABLE")
        error_message = "NO - PAPER MAKE AVAILABLE"
        views.statuslog(un,starttime,endtime,slot,error_message, 'CenterDelay')
