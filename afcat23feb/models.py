from django.db import models

# Create your models here.
from django.db import models

class exam_slot(models.Model):
    exam_slot_id = models.IntegerField(primary_key=True)
    exam_slot_starttime = models.DateTimeField()
    exam_slot_endtime = models.DateTimeField()
    exam_slot_code = models.CharField(max_length=255)
    exam_slot_name = models.CharField(max_length=255)
    record_tracking = models.TimeField()
    exam_slotreportingtime = models.CharField(max_length=255)
    exam_slot_status = models.BooleanField()
    exam_date = models.TimeField()
    exam_schedule_id = models.IntegerField()
    exam_slot_shift = models.CharField(max_length=255)

    class Meta:
        db_table = 'exam_slot'
        managed = 'postgresql'
