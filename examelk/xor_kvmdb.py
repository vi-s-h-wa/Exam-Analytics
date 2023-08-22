    #!/usr/bin/env python
    # coding: utf-8

    # In[3]:
def kvm(store,dbname):

    from IPython.core.interactiveshell import InteractiveShell
    InteractiveShell.ast_node_interactivity='all'

    # In[2]:


    # site:edid.tv kvm


    # In[6]:


    # !pip3 install BeautifulSoup4


    # In[2]:


    # print(soup.find('table'))


    # In[16]:


    ### TEST KVM from EDIT -- START WORKING --


    # In[35]:


    import requests
    from bs4 import BeautifulSoup

    # Define the base URL and the starting page to crawl
    base_url = "https://www.edid.tv"
    starting_page = "/manufacturer/"

    # Send a request to the starting page and get its content
    session = requests.Session()
    session.verify = False
    response = session.get(base_url+starting_page)
    html_content = response.content

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all links that start with /manufacturer/
    links = soup.find_all("a", href=lambda href: href.startswith("/manufacturer/"))

    # Loop through the links and crawl their content
    for link in links:
        page_url = base_url + link.get("href")
        response = session.get(page_url)
        page_content = response.content
        print(page_content)
        # do something with the page content


    # In[ ]:





    # ### KVM - 77 values

    # In[235]:



    import requests
    import pandas as pd
    from bs4 import BeautifulSoup    

    kvm_edid_df = pd.DataFrame()
    # url = 'https://www.fangraphs.com/projections.aspx?pos=all&stats=bat&type=fangraphsdc&team=0&lg=all&players=0'

    url = 'https://www.displayninja.com/kvm-switch-monitor-list/'

    response = requests.get(url, verify=False)

    # Use BeautifulSoup to parse the HTML code
    soup = BeautifulSoup(response.content, 'html.parser')

    # changes stat_table from ResultSet to a Tag
    stat_table = soup.find('table', {'id' : 'tablepress-145'})

    # Convert html table to list
    rows = []
    for tr in stat_table.find_all('tr')[1:]:
        cells = []
        tds = tr.find_all('td')
        if len(tds) == 0:
            ths = tr.find_all('th')
            for th in ths:
                cells.append(th.text.strip())
        else:
            for td in tds:
                cells.append(td.text.strip())
        rows.append(cells)

    # convert table to df
    table = pd.DataFrame(rows)
    kvm_edid_df = pd.concat([kvm_edid_df, table], ignore_index=True)
    kvm_edid_df = kvm_edid_df.iloc[:, 0:6]
    col = ['monitor', 'size', 'resolution', 'panel', 'refresh_rate', 'usb_c_pd']
    kvm_edid_df.columns = col
    kvm_edid_df.head(3)
    kvm_edid_df.loc[60,'monitor']= 'MSI MPG273CQR-QD'


    # In[237]:
    kvm_edid_df[['manufacturer', 'model', 'model_ext']] = kvm_edid_df['monitor'].str.split(n=2,expand=True) 
    kvm_edid_df['kvm']='yes'
    kvm_edid_df.head()
    # kvm_edid_df.iloc[59:62, ]


    # In[267]:


    kvm_edid_df.rename(columns={"monitor":"actual_name"}, inplace=True)
    kvm_edid_df.to_csv("kvm_display.csv", index=False)


    # In[268]:


    kvm_edid_df.model.value_counts()


    # In[ ]:


    ## Read KVM from CSV store into database


    # In[36]:


    import pandas as pd

    import importlib
    import config
    importlib.reload(config)


    # In[37]:

    from pymongo import MongoClient
    connection = MongoClient(store)
    db=connection[dbname]
    # db=connection['starmay_2023']


    # In[38]:


    kvm_edid_df = pd.read_csv('/home/cloud/kvm_display.csv')
    kvm_edid_df.reset_index(inplace=True)
    import json
    records = json.loads(kvm_edid_df.T.to_json()).values()
    db.edid_kvm.insert_many(records)


    # In[5]:


    # ### read from mongodb and compare
    # edid_compare = pd.DataFrame(list(db.edid.aggregate([{"$group":{"_id":{"id":"$id", "name":"$name", "model":"$Model"}}}, {"$project":{"_id":0, "id":"$_id.id", "name":"$_id.name", "model":"$_id.model"}}])))
    # edid_compare.head(2)
    # edid_compare.shape
    # # db.edid.find()


    # In[260]:


    # kvm_edid_df.manufacturer.value_counts()


    # In[261]:


    # edid_compare[edid_compare.model=='VES3700']


    # In[39]:


    import requests
    import pandas as pd
    from bs4 import BeautifulSoup    

    edid_df = pd.DataFrame()
    # url = 'https://www.fangraphs.com/projections.aspx?pos=all&stats=bat&type=fangraphsdc&team=0&lg=all&players=0'
    for i in range(1,9):
        
        url = 'https://edid.tv/manufacturer/?page='+str(i)

        response = requests.get(url, verify=False)

        # Use BeautifulSoup to parse the HTML code
        soup = BeautifulSoup(response.content, 'html.parser')

        # changes stat_table from ResultSet to a Tag
        stat_table = soup.find('table')

        # Convert html table to list
        rows = []
        for tr in stat_table.find_all('tr')[1:]:
            cells = []
            tds = tr.find_all('td')
            if len(tds) == 0:
                ths = tr.find_all('th')
                for th in ths:
                    cells.append(th.text.strip())
            else:
                for td in tds:
                    cells.append(td.text.strip())
            rows.append(cells)

        # convert table to df
        table = pd.DataFrame(rows)
        edid_df = pd.concat([edid_df, table], ignore_index=True)


    # In[40]:


    # table
    # edid_df.append(table)

    edid_df.columns = ['id', 'name', 'edid']
    edid_df.head(2)


    # In[9]:


    # from pymongo import MongoClient
    # connection = MongoClient("mongodb://10.184.61.202:27017")
    # db=connection['icgmar2023']
    # # db=connection['starmay_2023']


    # In[41]:


    edid_df.reset_index(inplace=True)
    import json
    records = json.loads(edid_df.T.to_json()).values()
    records
    db.edid.insert_many(records)


    # In[42]:


    ### https://raw.githubusercontent.com/linuxhw/EDID/master/DigitalDisplay.md
    ### data were cleaned using text editor
    edid_git = pd.read_csv("/home/cloud/edid_DigitalDisplay")
    edid_git.shape
    edid_git.head(4)

    edid_git = edid_git.rename(columns={'MFG':'id', "Name":'name', 'ID':'edid'} )
    import json
    records = json.loads(edid_git.T.to_json()).values()
    # records
    db.edid.insert_many(records)


    # ### Extract the first 3 char from Model - Get manufacturer lists

    # In[43]:


    edid_git = pd.read_csv("/home/cloud/edid_DigitalDisplay")
    edid_git.shape
    edid_git.head(4)
    edid_git = edid_git.rename(columns={'MFG':'id', "Name":'name', 'ID':'edid'} )
    edid_git['id'] = edid_git.Model.str[0:3]
    records = json.loads(edid_git.T.to_json()).values()
    # records
    db.edid.insert_many(records)


    # #### new records manual input

    # In[114]:


    test_list = [['CVT','AMG'], ['AA','BB','CC']]
    pd.DataFrame(test_list, columns=['col_A', 'col_B', 'col_C'])

    # pd.DataFrame('')


    # In[44]:


    ### https://chromium.googlesource.com/chromiumos/third_party/libsigrokdecode/+/master/decoders/edid/pnpids.txt
    ### data were cleaned using text editor
    edid_git = pd.read_csv("/home/cloud/edid_chromiumos", names=['id', 'name'])
    edid_git.shape
    edid_git.head(4)

    import json
    records = json.loads(edid_git.T.to_json()).values()
    # records
    db.edid.insert_many(records)

