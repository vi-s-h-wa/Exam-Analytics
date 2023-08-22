from examelk import views
from pymongo import MongoClient
import pandas as pd
import json
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity='all'
from IPython import get_ipython
def xorkvm(starttime,endtime,slot,un):
##changes ##cell added
# def quickFinisher():

    details = views.getdetails(un)
    print('xorkvm')    
    connection = MongoClient("mongodb://{0}:{1}".format(details['mip'],details['mport']))
    db=connection[details['mdbname']]
    print("mongodb://{0}:{1}".format(details['mip'],details['mport']))

    edid_df = pd.DataFrame(list(db.edid.find()))


# In[154]:


    edid_exam = pd.DataFrame(list(db.xor_hist.find({}, {'manufacturer':1, '_id':0})))
    edid_exam.shape
    edid_exam = pd.DataFrame(list(db.xor_hist.find({}, {'manufacturer':1, '_id':0})))
    edid_exam.shape


    # In[155]:


    edid_exam = edid_exam.applymap(lambda x: x.strip() if isinstance(x, str) else x)


    # In[156]:


    # len(edid_exam[edid_exam['manufacturer'].str.contains(' ACR  ')])
    len(edid_df[edid_df['id'].str.contains('ACR')])
    # df[df['date'].str.contains('07311954')]


    # In[157]:


    def verifyEDID(mname):
    #     print(mname)
        #     if(len(edid_exam[edid_exam['manufacturer'].str.contains(mname)])) >=1:
        if(len(edid_df[edid_df['id'].str.contains(mname)])) >=1:
            pass
    #         print( 'found')
            return 'found'
        else:
            print( 'EDID MANUFACTURE NOT FOUND : ' + mname    )
            return mname


    # In[158]:


    # edid_df['id'].apply(verifyEDID) 

    # edid_exam['manufacturer'].apply(verifyEDID) 


    # In[159]:


    lst_mname = []
    for vedid in edid_exam['manufacturer'].value_counts().index.tolist():
    #     print(vedid)
        lst_mname.append(verifyEDID(vedid.strip()))  
        
    # dataframe[column].value_counts().index.tolist()


    # In[160]:


    lst_new_mname = []
    for n in lst_mname:
        if n !='found':
            lst_new_mname.append(n)
    #         print(n)


    # In[161]:


    print("Not found from edid.tv lists : ", len(lst_new_mname), " total edid in edid.tv : ", edid_df.shape[0] )
    sorted(lst_new_mname)


    # In[162]:


    # records = json.loads(edid_df.T.to_json()).values()
    # records
    # db.edid.insert_many(records)
    db.edid_exam.remove()
    db.edid_exam.insert_many(json.loads(pd.DataFrame(lst_new_mname, columns=['name']).T.to_json()).values())


    # In[163]:


    # get_ipython().run_cell_magic('time', '', "lst_edid_all = edid_df.id.tolist()\n\nlst_edid_examOnly = edid_exam[~edid_exam.manufacturer.isin(lst_edid_all)].manufacturer.unique().tolist()\n# edid_exam.merge(edid_df , left_on='manufacturer', right_on= 'id')")
    lst_edid_all = edid_df.id.tolist()
    lst_edid_examOnly = edid_exam[~edid_exam.manufacturer.isin(lst_edid_all)].manufacturer.unique().tolist() 
    edid_exam.merge(edid_df , left_on='manufacturer', right_on= 'id')


    # In[164]:


    xor_hist_df = pd.DataFrame(list(db.xor_hist.find({})))
    xor_hist_df.head(2)


    # In[165]:


    xor_hist_df.shape
    xor_hist_df.drop_duplicates(['centercode', 'cip','cmac', 'manufacturer', 'mod', 'mon', 'serialno'], inplace=True)
    xor_hist_df.shape


    # In[166]:


    # xor_hist_df[~xor_hist_df.manufacturer.isin(lst_edid_all)].shape
    xor_hist_examonly = xor_hist_df[xor_hist_df.manufacturer.isin(lst_edid_examOnly)]
    xor_hist_examonly['is_alert'] = 'examonly'
    xor_hist_examonly.shape


    # In[167]:


    xor_hist_examonly1 = xor_hist_df[~xor_hist_df.manufacturer.isin(lst_edid_examOnly)]
    xor_hist_examonly1['is_alert']='edid'
    xor_hist_examonly = pd.concat([xor_hist_examonly,xor_hist_examonly1])
    xor_hist_examonly.shape

    # get_ipython().run_cell_magic('time', '', 'records = json.loads(xor_hist_examonly.T.to_json(default_handler=str)).values()\n# \n# db.xor_hist_examonly.remove()\ndb.xor_hist_examonly.insert_many(records)\n\n\n##### STOP HERE')
    records = json.loads(xor_hist_examonly.T.to_json(default_handler=str)).values()
    db.xor_hist_examonly.remove()
    db.xor_hist_examonly.insert_many(records)

    
