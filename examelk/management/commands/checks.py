from django.core.management.base import BaseCommand
import datetime
from django.utils import timezone
import time
from examelk import views
from examelk.models import *
from examelk import configs
import pymongo
from examelk import slowstart, quickfinish, centerdelay, canomaly, CSMG, xor, xornan, mal, xor_kvm
class Command(BaseCommand):
    help = 'Run the custom script every time the development server is started.'

    def handle(self, *args, **options):
        time_difference = datetime.timedelta(hours=5, minutes=30)
        while True:
            all_records = Scripts.objects.all()
            current_time = timezone.now()
            print(current_time)
            for scripts_obj in all_records:
                start_time = scripts_obj.starttime + time_difference
                end_time = scripts_obj.endtime + time_difference
                start_date_hrs_mins = start_time.replace(second=0, microsecond=0)
                current_date_hrs_mins = current_time.replace(second=0, microsecond=0)
                if scripts_obj.status == 'TRUE' and current_time > end_time:
                    print('TRUE')
                    stored="mongodb://"+configs.storemongodbhost+":"+str(configs.storemongodbport)+"/"
                    client = pymongo.MongoClient(stored)
                    db = client[configs.storemongodbname]
                    collection = db["examelk_scripts"]
                    collection.update_one({'id': scripts_obj.id},{'$set': {'status': 'FALSE'}})
                if scripts_obj.status == 'TRUE':
                    if start_date_hrs_mins < current_date_hrs_mins :
                        if current_time <= end_time:
                            print(start_time,end_time)
                            flag =  0
                            i = 0
                            print(abs(current_time.minute - start_time.minute))
                            startt = scripts_obj.starttime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            endt = scripts_obj.endtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            dayst = scripts_obj.daystarttime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            dayet = scripts_obj.dayendtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            uaware = datetime.datetime.strptime(startt, "%Y-%m-%dT%H:%M:%S.%fZ") +  time_difference
                            unaware_str = uaware.strftime("%Y-%m-%d %H:%M:%S")
                            unaware = datetime.datetime.strptime(unaware_str, "%Y-%m-%d %H:%M:%S") 
                            actual = datetime.datetime.fromisoformat(str(scripts_obj.endtime)) + time_difference
                            factual = actual.strftime("%B %d, %Y, %I:%M:%S %p")
                            if abs(current_time.minute - start_time.minute) == scripts_obj.interval and flag==0:
                                try:
                                    quickfinish.quickfinishfn(startt,endt,scripts_obj.slot,scripts_obj.username,scripts_obj.index)
                                    print('quick')
                                except Exception as e:
                                    function_name = 'quickfinishfn'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                try:
                                    slowstart.slowstartfn(startt,endt,scripts_obj.slot,scripts_obj.username,scripts_obj.index)
                                    
                                except Exception as e:
                                    function_name = 'slowstart'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in slowstartfn:", str(e))
                                try:
                                    mal.malfn(startt,endt,dayst,scripts_obj.slot,scripts_obj.username,scripts_obj.index,scripts_obj.malindex)
                                    
                                except Exception as e:
                                    function_name = 'MAL'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in slowstartfn:", str(e))
                                try:
                                    centerdelay.centerdelayfn(startt,endt,scripts_obj.slot, unaware, factual,scripts_obj.username,scripts_obj.index)
                                except Exception as e:
                                    function_name = 'centerdelay'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in centerdelayfn:", str(e))
                                try:
                                    CSMG.csmgfn(startt, endt,dayst,scripts_obj.slot,scripts_obj.username,scripts_obj.index)
                                except Exception as e:
                                    function_name = 'csmg'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in csmgfn:", str(e))
                                try:
                                    xor.xorfn(startt,endt, dayst, dayet,scripts_obj.slot,scripts_obj.username,scripts_obj.index)
                                except Exception as e:
                                    function_name = 'xor'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in xorfn:", str(e))
                                try:
                                    xornan.xornanfn(startt,endt, dayst, dayet,scripts_obj.slot,scripts_obj.username,scripts_obj.index)
                                except Exception as e:
                                    function_name = 'xornan'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in xornanfn:", str(e))
                                try:
                                    xor_kvm.xorkvm(startt,endt,scripts_obj.slot,scripts_obj.username)
                                except Exception as e:
                                    function_name = 'xor_kvm'
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    print("An error occurred in xorkvm:", str(e))
                                flag=1
                            diff = abs(current_time.minute - start_time.minute)
                            if diff % scripts_obj.interval == 0:
                                st = scripts_obj.starttime
                                inter = datetime.timedelta(minutes=i)
                                new_time = st + inter
                                new_time_str = new_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                                try:
                                    print(scripts_obj.starttime)
                                    print(type(unaware))
                                    canomaly.canomalyfn(new_time_str,endt,dayet, scripts_obj.slot,scripts_obj.username,scripts_obj.index,scripts_obj.ansindex)
                                except Exception as e:
                                    error_message = str(e)
                                    views.statuslog(scripts_obj.username,startt,endt,scripts_obj.slot,str(e), function_name)
                                    i = i + scripts_obj.interval
                                    print("An error occurred in canomalyfn:", str(e))
            time.sleep(60)
            current_time = timezone.now()