# Generated by Django 4.1.9 on 2023-07-18 16:04

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('navyjuly', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_date',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_schedule_id',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slot_endtime',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slot_name',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slot_shift',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slot_starttime',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slot_status',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='exam_slotreportingtime',
        ),
        migrations.RemoveField(
            model_name='exam_slot',
            name='record_tracking',
        ),
    ]
