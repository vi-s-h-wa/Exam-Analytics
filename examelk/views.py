from urllib.parse import quote
from django.shortcuts import render, redirect
from django.views import View
from examelk.models import *
from navyjuly.models import *
from django.http import JsonResponse
import sys
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd
import pymongo
import json
from pymongo import MongoClient
import time
from django.db.utils import ConnectionDoesNotExist
from datetime import datetime, tzinfo, timedelta
from django.http import HttpResponseRedirect
from examelk import configs
from examelk import slowstart, quickfinish, quickfinishicg,centerdelay,canomaly,CSMG, mac, xor, xornan, mal ,xor_kvmdb, xor_kvm
ES_CERTIFICATES = {'ca_certs': '/usr/local/share/ca-certificates/elastic1/ca.crt',
                   'client_certs': '/usr/local/share/ca-certificates/elastic1/logclient.crt', 'client_key': '/usr/local/share/ca-certificates/elastic1/logclient.key'}
from django.http import JsonResponse
from pymongo import MongoClient


def create_slot(request):
    if request.method == 'POST':
        slots = request.POST.get('slots')
        slots_list = slots.split(',')
        for slot in slots_list:
            Slot.objects.create(
                slot = slot
            )
        return JsonResponse({'message': 'slots created successfully!'})
    else:
        return JsonResponse({'message': 'Invalid request method.'})

def kvm(request):
    dbname = str(request.POST.get('dbname'))
    host = str(request.POST.get('host'))
    port = str(request.POST.get('port'))
    store = 'mongodb://'+host+':'+port
    xor_kvmdb.kvm(store,dbname)
    return JsonResponse({'message': ''})

def delete_collections(request):
    if request.method == 'POST':
        collections = request.POST.get('collections')
        dbname = str(request.POST.get('dbname'))
        collection_list = collections.split(',')  # Split the comma-separated list into individual collection names
        stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
        client = MongoClient(stored)
        db = client[dbname]

        for collection_name in collection_list:
            db[collection_name].drop()  # Delete each collection in the list

            return JsonResponse({'message': 'Collections deleted successfully!'})
    else:
        return JsonResponse({'message': 'Invalid request method.'})


class Edit(View):
    def get(self, request):
        un = request.GET.get('username')
        stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
        client = pymongo.MongoClient(stored)
        db = client[configs.storemongodbname]
        collection = db["examelk_examconfig"]
        document = collection.find_one({"username": un})
        context = {
                    'examcode' : document.get('examcode'),
                    'ipaddress' : document.get('ipaddress'),
                    'port' : document.get('port'),
                    'eusername' : document.get('eusername'),
                    'epassword' : document.get('epassword'),
                    'host' : document.get('host'),
                    'mport' : document.get('mport'),
                    'musername' : document.get('musername'),
                    'mpassword' : document.get('mpassword'),
                    'mdbname' : document.get('mdbname'),
                }
        return render(request,'examelk/edit.html',context)
    
    def post(self, request):
        un = request.GET.get('username')
        ecode = request.POST.get('examcode')
        elkip = request.POST.get('ipaddress')
        elkport = request.POST.get('port')
        elkuname = request.POST.get('eusername')
        elkpwd = request.POST.get('epassword')
        mip = request.POST.get('host')
        mport = request.POST.get('mport')
        muname = request.POST.get('musername')
        mpwd = request.POST.get('mpassword')
        dbname = request.POST.get('mdbname')
        stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
        client = pymongo.MongoClient(stored)
        db = client[configs.storemongodbname]
        collection = db["examelk_examconfig"]
        cli = pymongo.MongoClient("mongodb://"+str(mip)+":"+str(mport)+"/")
        dbs = cli[dbname]
        collection_exists = 'edid' not in dbs.list_collection_names()
        # Update MongoDB documents with matching index
        query = {'username': un}  # Assuming 'index' is the field used for matching
        update = {
            '$set': {
                'examcode': ecode,
                'ipaddress': elkip,
                'port': elkport,
                'eusername': elkuname,
                'epassword': elkpwd,
                'host': mip,
                'mport': mport,
                'musername': muname,
                'mpassword': mpwd,
                'mdbname': dbname,
            }
        }
        collection.update_many(query, update)
        context = {
                    'collection_exists' : collection_exists,
                    'un' : un,
                    'examcode' : ecode,
                    'ipaddress' : elkip,
                    'port' : elkport,
                    'eusername' : elkuname,
                    'epassword' : elkpwd,
                    'host' : mip,
                    'mport' : mport,
                    'musername' : muname,
                    'mpassword' : mpwd,
                    'mdbname' : dbname,
                }
        return render(request, 'examelk/document.html',context)
    
class login(View):
    def get(self, request):
        if request.session.get('authenticated'):
            try:
                un = request.session['username']
                data = ExamConfig.objects.all().filter(username = un)
                for d in data:
                    cli = pymongo.MongoClient("mongodb://"+str(d.host)+":"+str(d.mport)+"/")
                    dbs = cli[d.mdbname]
                    collection_exists = 'edid' not in dbs.list_collection_names()
                    context = {
                            'collection_exists' : collection_exists,
                            'un' : d.username,
                            'examcode' : d.examcode,
                            'ipaddress' : d.ipaddress,
                            'port' : d.port,
                            'eusername' : d.eusername,
                            'epassword' : d.epassword,
                            'host' : d.host,
                            'mport' : d.mport,
                            'musername' : d.musername,
                            'mpassword' : d.mpassword,
                            'mdbname' : d.mdbname,
                            }
                return render(request, 'examelk/document.html' , context)
            except:
                return render(request,'examelk/login.html')
        return render(request,'examelk/login.html')
    
    def post(self, request):
        username = request.POST.get('un')
        password = request.POST.get('pass')
        if username == configs.superun and password == configs.superpass:
            Config = ExamConfig.objects.all()
            config_data = list(Config.values())
            request.session['config_data'] = config_data
            return HttpResponseRedirect('config/')
        stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
        client = pymongo.MongoClient(stored)
        db = client[configs.storemongodbname]
        collection = db["examelk_examconfig"]
        document = collection.find_one({"username": username})
        if document:
            stored_password = document.get("password")
            if stored_password == password:
                request.session['authenticated'] = True
                request.session['username'] = document.get('username')
                try:
                    cli = pymongo.MongoClient("mongodb://"+str(document.get('host'))+":"+str(document.get('mport'))+"/")
                    dbs = cli[document.get('mdbname')]
                    collection_exists = 'edid' not in dbs.list_collection_names()
                except:
                    collection_exists = True
                context = {
                    'collection_exists' : collection_exists,
                    'un' : document.get('username'),
                    'examcode' : document.get('examcode'),
                    'ipaddress' : document.get('ipaddress'),
                    'port' : document.get('port'),
                    'eusername' : document.get('eusername'),
                    'epassword' : document.get('epassword'),
                    'host' : document.get('host'),
                    'mport' : document.get('mport'),
                    'musername' : document.get('musername'),
                    'mpassword' : document.get('mpassword'),
                    'mdbname' : document.get('mdbname'),
                }
                return render(request, 'examelk/document.html', context)
            else:
                return render(request, 'examelk/login.html', {'msg':'Credentials Invalid!!'})
        else:
            return render(request, 'examelk/login.html', {'msg':'Credentials Invalid!!'})

class Logout(View):
    def get(self, request):
        request.session.clear()
        return redirect('login')
        
# Create your views here.
class HomeTemplate(View):
 
    def get(self, request):
        config_data = request.session.get('config_data')
        if config_data:
            # Deserialize the config_data back to a list of dictionaries
            return render(request, 'examelk/home.html', {'config_data': config_data})
        context = {
            'det': ExamConfig.objects.all(),
        }
        return render(request, 'examelk/home.html', context)
    def post(self, request):
        ecode = request.POST.get('ecode')
        elkip = request.POST.get('ip')
        elkport = request.POST.get('port')
        elkuname = request.POST.get('euser')
        elkpwd = request.POST.get('epassword')
        file = request.FILES['file1']
        file2 = request.FILES['file2']
        file3 = request.FILES['file3']
        file4 = request.FILES['file4']
        mip = request.POST.get('mip')
        mport = request.POST.get('mport')
        muname = request.POST.get('muname')
        mpwd = request.POST.get('mpwd')
        dbname = request.POST.get('dbname')
        username = request.POST.get('Username')
        password = request.POST.get('password')
        try:
            cli = pymongo.MongoClient("mongodb://"+str(mip)+":"+str(mport)+"/")
            dbs = cli[dbname]
            collection_exists = 'edid' not in dbs.list_collection_names()
        except:
            collection_exists = False
        context = {
                    'collection_exists' : collection_exists,
                    'un' : username,
                    'examcode' : ecode,
                    'ipaddress' : elkip,
                    'port' :    elkport,
                    'eusername' : elkuname,
                    'epassword' : elkpwd,
                    'host' : mip,
                    'mport' : mport,
                    'musername' : muname,
                    'mpassword' : mpwd,
                    'mdbname' : dbname,
                    'ecode': ecode ,
                }
        store = 'mongodb://'+mip+':'+mport
        print('hii')
        try:
            ExamConfig.objects.create(
                examcode= ecode, ipaddress=elkip, port=elkport, epassword=elkpwd,host=mip, mport=mport, musername=muname, mpassword=mpwd, 
                mdbname=dbname, username = username, password = password,eusername = elkuname,file=file,file2=file2,file3=file3,file4=file4
            )
            xor_kvmdb.kvm(store,dbname)
        except Exception as det:
            print(det)

        return render(request, 'examelk/document.html', context)


class simple_utc(tzinfo):
    def tzname(self,**kwargs):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

def convertdate(date_str):
    # Convert input string to datetime object
    datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S" if len(date_str) > 16 else "%Y-%m-%dT%H:%M")

    # Add a default value of zero seconds if not provided
    if datetime_obj.second == 0:
        datetime_obj = datetime_obj.replace(second=0)

    # Format the datetime object to the desired format
    formatted_date = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    return formatted_date




from pymongo import MongoClient


def getdetails(un):
    # Connect to MongoDB
    stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
    client = pymongo.MongoClient(stored)
    db = client[configs.storemongodbname]
    collection = db["examelk_examconfig"]
    document = collection.find_one({"username": str(un) })

    # Extract the details from the retrieved document
    if document:
        details = {
            'elkip': document['ipaddress'],
            'elkport': document['port'],
            'elkuname': document['eusername'],
            'elkpwd': document['epassword'],
            'mip': document['host'],
            'mport': document['mport'],
            'muname': document['musername'],
            'mpwd': document['mpassword'],
            'mdbname': document['mdbname']
        }
    else:
        details = {}  # Return an empty dictionary if no document is found

    return details
class Manual(View):

    def get(self, request, *args, **kwargs):
        ecode = request.GET.get('ecode')
        try:
            data = exam_slot.objects.using('postgresql').values(configs.postgrescolumnname)
        except:
            data = []
        data1 = Slot.objects.values('slot')
        context = {
            'ecode': ecode,
            'data' : data,
            'data1' : data1,
        }
        return render(request, 'examelk/manual.html', context)
    
    def post(self, request, *args, **kwargs):
        import datetime
        import time
        etype = request.POST.get('etype')
        papercode = request.POST.get('papercode')
        index = request.POST.get('index')
        ansindex = request.POST.get('ansindex')
        malindex = request.POST.get('malindex') 
        examstarttime = convertdate(request.POST.get('estime'))
        time_difference = datetime.timedelta(hours=5, minutes=30)
        unaware = datetime.datetime.fromisoformat(request.POST.get('estime')) + time_difference
        actual = datetime.datetime.fromisoformat(request.POST.get('estime')) + time_difference
        factual = actual.strftime("%B %d, %Y, %I:%M:%S %p")
        examendtime = convertdate(request.POST.get('eetime'))
        querystarttime = convertdate(request.POST.get('qstime'))
        queryendtime = convertdate(request.POST.get('qetime'))
        slot = request.POST.get('slot')
        scripttorun = list(map(str, request.POST.getlist('scripttorun')))
        un = request.GET.get('un')
        
   
        for script in scripttorun:
            if script  == 'quickfinish':
                quickfinish.quickfinishfn(examstarttime,examendtime,slot,un,index)
            elif script == 'CSMG':
                CSMG.csmgfn(examstarttime,examendtime,querystarttime,slot,un,index)
            elif script == 'centerdelay':
                centerdelay.centerdelayfn(examstarttime,examendtime,slot,unaware,factual,un,index)
            elif script == 'canomaly':
                canomaly.canomalyfn(examstarttime,examendtime,queryendtime,slot,un,index,ansindex)
            elif script == 'slowstart':
                slowstart.slowstartfn(examstarttime,examendtime,slot,un,index)
            elif script == 'mal':
                mal.malfn(examstarttime,examendtime,querystarttime,slot,un,index,malindex)
            elif script == 'mac':
                mac.macfn(querystarttime,un,index)
            elif script == 'xor':
                xor.xorfn(examstarttime, examendtime,querystarttime,queryendtime,slot,un,index)
            elif script == 'xornan':
                xornan.xornanfn(examstarttime, examendtime,querystarttime,queryendtime,slot,un,index)
            elif script == 'xor_kvm':
                xor_kvm.xorkvm(examstarttime, examendtime,slot,un)
        return redirect('/')

def statuslog(username,examstarttime, examendtime, slot, errormessage, functionname):
    error_log = Error(
        username = username,
        slot = slot,
        script_name=functionname,
        error_time=str(examstarttime) + ' - ' + str(examendtime),
        error_message=errormessage
    )
    error_log.save()


class ErrorView(View):

    def get(self, request, *args, **kwargs):
        un = request.GET.get('username')
        data = Error.objects.all().filter(username = un).order_by('slot','script_name','-error_time')
        d = []
        for x in data:
            if x.slot not in d:
                d.append(x.slot)
        context = {
            'data' : data,
            'd' : d,
        }
        return render(request, 'examelk/errors.html', context)

class ExamScript(View):

    def get(self, request, *args, **kwargs):
        ecode = request.GET.get('ecode')
        un = request.GET.get('un')
        Script = Scripts.objects.all().filter(username = un)
        try:
            data = exam_slot.objects.using('postgresql').values('exam_slot_code')
        except:
            data = []
        data1 = Slot.objects.values('slot')
        context = {
            'ecode': ecode,
            'data' : data,
            'data1' : data1,
            'script' : Script,
            'un' : un,
        }
        return render(request, 'examelk/examscript.html', context)

    def post(self, request, *args, **kwargs):
        import datetime
        import time
        etype = request.POST.get('etype')
        index = request.POST.get('index')
        ansindex = request.POST.get('ansindex')
        malindex = request.POST.get('malindex')
        interval = int(request.POST.get('interval'))
        papercode = request.POST.get('papercode')
        examstarttime = convertdate(request.POST.get('estime'))
        examendtime = convertdate(request.POST.get('eetime'))
        querystarttime = convertdate(request.POST.get('qstime'))
        queryendtime = convertdate(request.POST.get('qetime'))
        slot = request.POST.get('slot')
        un = request.POST.get('un')
        #scripttorun = list(map(str, request.POST.getlist('scripttorun')))
        Scripts.objects.create(
                username = un,starttime = examstarttime, endtime=examendtime, daystarttime = querystarttime, dayendtime = queryendtime, slot=slot, index = index,interval = interval,malindex =malindex,ansindex = ansindex,status = 'TRUE'
            )
       
        return redirect('/')

   
        '''for script in scripttorun:
            if script  == 'quickfinish':
                print('quickfinish')
                #print(quickfinishicg.quickfinishicgfn(examstarttime,examendtime,slot,papercode))
                quickfinish.quickfinishfn(examstarttime,examendtime,slot,papercode,index)
            elif script == 'CSMG':
                CSMG.csmgfn(querystarttime,slot,index)
            elif script == 'centerdelay':
                centerdelay.centerdelayfn(examstarttime,slot,unaware,factual,index)
            elif script == 'canomaly':
                print('canomaly')
                canomaly.canomalyfn(examstarttime,queryendtime,slot,index)
            elif script == 'slowstart':
                slowstart.slowstartfn(examstarttime,examendtime,slot,papercode,index)
            elif script == 'mac':
                mac.macfn(querystarttime,index)
            elif script == 'xor':
                xor.xorfn(examstarttime, examendtime,querystarttime,queryendtime,slot,index)
            elif script == 'xornan':
                xornan.xornanfn(examstarttime, examendtime,querystarttime,queryendtime,slot,index)
        return redirect('/')'''