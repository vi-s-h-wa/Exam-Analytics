from django.db import models

# Create your models here.
from django.db import models

class exam_slot(models.Model):
    exam_slot_code = models.CharField(max_length=255)
    class Meta:
        db_table = 'exam_slot'
        managed = 'postgresql'
