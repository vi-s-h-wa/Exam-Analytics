
### this is the credetials for creating New Exam 
superun = 'superadmin'
superpass = 'superadmin'


# -- database server for storing user and  exam configurations
storemongodbhost = 'localhost'
storemongodbname = 'exam'
## Port must be integer value
storemongodbport = 27017

#Read exam slot info. database must have table exam_slot and must have column exam_slot_code 
## if no data provided with Postgresql Database, please create collection "exam_slot" under database variable storemongodbname
### A collection must have key  exam_slot_code, sample values are {exam_slot_code:'A'}, {exam_slot_code:'B'} 
postgresdbname  = 'afcat23feb'
postgresdbusername  = 'postgres'
postgresdbpassword  = 'postgres'
postgreshost    = '10.184.43.111'
postgresport    = '5432'
postgrescolumnname = 'exam_slot_code'

#To access a the exam_slot_code from a new postgres database
##first navigate to the directory by connecting via ssh.
#ssh cloud@10.184.49.216. Once entered enter - cd /opt/examgui/examanalytics 
##reaching the destination, start a new django app - python3 manage.py startapp 'postgresdbname'
## now the application will be created in the project's dir. Right after go to (examanalytics) settings.py, where you will find a list called INSTALLED_APPS , add the name of the database.
#  
# In this config file change the postgresdbname.
## Now under the 'new application folder' (name which u have added in INSTALLED_APPS --> postgresdbname) go to models.py file. In models.py create the model name as the table name and the define the column in the models.
'''
Example:

class exam_slot(models.Model):
    exam_slot_code = models.CharField(max_length=255)
    class Meta:
        db_table = 'exam_slot'
        managed = 'postgresql'

here exam_slot is the table name, while exam_slot_code is the column name. 
Change the 'postgrescolumnname' in configs And then the data will be retrieved.        


'''
### MESSAGES

submit_answer = 'submitAnswer' # [canomaly]
start_exam = 'startExamination' # [csmg, mal, quickfinish, slowstart, xor, xornan]
make_paper = 'SUCCESS: Make Paper Available' # [centerdelay, slowstart]
candidate_submitted = 'Candidate Submitted Examination' # [quickfinish]
