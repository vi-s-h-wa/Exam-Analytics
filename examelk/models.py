from django.db import models

# Create your models here.
class ExamConfig(models.Model):
    examcode = models.CharField(max_length=255, default=None)
    ipaddress = models.CharField(max_length=255)
    port = models.IntegerField()
    eusername = models.CharField(max_length=255, default=None)
    epassword = models.CharField(max_length=255, default=None)
    host = models.CharField(max_length=255)
    mport = models.IntegerField()
    musername = models.CharField(max_length=255)
    mpassword = models.CharField(max_length=255)
    mdbname = models.CharField(max_length=255)
    username = models.CharField(max_length=255, unique=True , default=None)
    password = models.CharField(max_length=255)
    file = models.FileField(upload_to="files", default=None)
    file2 = models.FileField(upload_to="files", default=None)
    file3 = models.FileField(upload_to="files", default=None)
    file4 = models.FileField(upload_to="files", default= None)

class Error(models.Model):
    username = models.CharField(max_length=127, default = None)
    slot = models.CharField(max_length=127, default = None)
    script_name = models.CharField(max_length=255, default=None)
    error_time = models.CharField(max_length=255, default=None)
    error_message = models.CharField(max_length=255, default=None)

class Scripts(models.Model):
    id = models.AutoField(primary_key=True)
    username = models.CharField(max_length=255, default=None)
    index       = models.CharField(max_length=255)
    ansindex       = models.CharField(max_length=255, default=None)
    malindex       = models.CharField(max_length=255, default=None)
    starttime = models.DateTimeField(default=None)
    endtime = models.DateTimeField(default=None)
    daystarttime = models.DateTimeField(default=None)
    dayendtime = models.DateTimeField(default=None)
    slot = models.CharField(max_length=127, default=None)
    interval = models.IntegerField(max_length=127, default=None)
    status = models.CharField(max_length=127, default=None)

class Slot(models.Model):
    slot = models.CharField(max_length=127, default=None)