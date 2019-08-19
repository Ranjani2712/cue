import update_db_script as updb
import datetime
import os
import json
import sys

file_prefix = "db_log_"
default_date = datetime.date(2018, 1, 20)
default_sync_obj = {
					'items':default_date, \
					'activities':default_date, \
					'sessions':default_date,\
					'students':default_date,\
					'teachers':default_date,\
					'classrooms':default_date,\
					'update_items':True,\
					'update_activities':True,\
					'update_sessions':True,\
					'update_students':True,\
					'update_teachers':True,\
					'update_classrooms':True,
					}

def retrieve_last_sync(directory):
	# Get the Last_Successful_Sync File and return its data
	# Get all files in DB_Logs; Get latest (sort by date-time) datetimes to look ahead from.
	toggle_item = False
	toggle_activity = False
	toggle_session = False
	toggle_student = False
	toggle_teacher = False
	toggle_classroom = False
	try:
		db_status_log_list = [f for f in os.listdir(directory) if f.find(file_prefix)!=-1]
	except FileNotFoundError:
		os.makedirs(directory)
		db_status_log_list=[]
	#print(db_status_log_list)
	if(len(db_status_log_list) == 0):
		last_sync_obj = default_sync_obj
		return(last_sync_obj)
	db_status_log_list.sort()
	last_sync_obj = {'update_items':False,\
					'update_activities':False,\
					'update_sessions':False,\
					'update_students':False,\
					'update_teachers':False,\
					'update_classrooms':False,
					}
	for idx in range(len(db_status_log_list), 0, -1):
		print((idx-1), db_status_log_list[(idx-1)])
		try:
			with open(directory + db_status_log_list[(idx-1)], "r") as fp:
				last_sync_status = json.load(fp)
				last_sync_dt = datetime.datetime.strptime(last_sync_status['dt_sync_started'], "%Y-%m-%d %H:%M:%S.%f") 
				#print("LAST_STATUS", last_sync_status)
		except json.decoder.JSONDecodeError:
			if((idx-1) == 0):
				last_sync_obj = default_sync_obj
				return(last_sync_obj)
			else:
				continue
		if(last_sync_status['sync_completed'] and not (toggle_item or toggle_activity or toggle_session or toggle_student or toggle_teacher or toggle_classroom)):
			last_sync_obj = {
							'items':last_sync_dt,
							'activities':last_sync_dt,
							'sessions':last_sync_dt,
							'students':last_sync_dt,
							'teachers':last_sync_dt,
							'classrooms':last_sync_dt - datetime.timedelta(days = 1),
							'update_items':True,
							'update_activities':True,
							'update_sessions':True,
							'update_students':True,
							'update_teachers':True,
							'update_classrooms':True,
							}
			toggle_item = True
			toggle_activity = True
			toggle_session = True
			toggle_student = True
			toggle_teacher = True
			toggle_classroom = True
			break
		else:
			if(last_sync_status['items_updated'] and not toggle_item):
				#print("Items Updated last time")
				last_sync_obj['items'] = last_sync_dt
				last_sync_obj['update_items'] = True
				toggle_item = True
			if(last_sync_status['activities_updated'] and not toggle_activity):
				#print("Activities Updated last time")
				last_sync_obj['activities'] = last_sync_dt
				last_sync_obj['update_activities'] = True
				toggle_activity = True
			if(last_sync_status['sessions_updated'] and not toggle_session):
				#print("Sessions Updated last time")
				last_sync_obj['sessions'] = last_sync_dt
				last_sync_obj['update_sessions'] = True
				toggle_session = True
			if(last_sync_status['students_updated'] and not toggle_student):
				#print("Sessions Updated last time")
				last_sync_obj['students'] = last_sync_dt
				last_sync_obj['update_students'] = True
				toggle_student = True
			if(last_sync_status['teachers_updated'] and not toggle_teacher):
				#print("Sessions Updated last time")
				last_sync_obj['teachers'] = last_sync_dt
				last_sync_obj['update_teachers'] = True
				toggle_teacher = True
			if(last_sync_status['classrooms_updated'] and not toggle_classroom):
				#print("Sessions Updated last time")
				last_sync_obj['classrooms'] = last_sync_dt
				last_sync_obj['update_classrooms'] = True
				toggle_classroom = True
		if(last_sync_obj['update_items'] and last_sync_obj['update_activities'] and \
			last_sync_obj['update_sessions'] and last_sync_obj['update_students'] and \
			last_sync_obj['update_teachers'] and last_sync_obj['update_classrooms']):
			break
	try:
		dt = last_sync_obj['items']
	except KeyError:
		last_sync_obj['items'] = default_date
		last_sync_obj['update_items'] = True
	try:
		dt = last_sync_obj['activities']
	except KeyError:
		last_sync_obj['activities'] = default_date
		last_sync_obj['update_activities'] = True
	try:
		dt = last_sync_obj['sessions']
	except KeyError:
		last_sync_obj['sessions'] = default_date
		last_sync_obj['update_sessions'] = True
	try:
		dt = last_sync_obj['students']
	except KeyError:
		last_sync_obj['students'] = default_date
		last_sync_obj['update_students'] = True
	try:
		dt = last_sync_obj['teachers']
	except KeyError:
		last_sync_obj['teachers'] = default_date
		last_sync_obj['update_teachers'] = True
	try:
		dt = last_sync_obj['classrooms']
	except KeyError:
		last_sync_obj['classrooms'] = default_date
		last_sync_obj['update_classrooms'] = True
	return(last_sync_obj)

	
def sync_complete_update(status, directory):
	# Write sync log to file. 
	status['dt_sync_completed'] = str(status['dt_sync_completed'])
	status['dt_sync_started'] = str(status['dt_sync_started'])
	fl_nm = file_prefix + str(datetime.date.today()).replace('-','_') + ".json"
	print("SYNC COMPLETE WITH",status)
	with open(directory+fl_nm, "w") as fp:
		json.dump(status, fp, indent = 4)


def check_sync_response(status):
	# Check sync object given
	# Depending on the status, update Last_Successful_Sync file
	return(status['sync_completed'])

def send_sync_error_report(status, directory):
	# Send email on the database sync error that occured.
	print("Need to send mail")
	pass

if __name__=="__main__":
	db_instance_nm = sys.argv[1]
	if(db_instance_nm == 'local'):
		subdir = "./LocalDB/"
	elif(db_instance_nm == 'dev'):
		subdir = "./DevDB/"
	elif(db_instance_nm == 'prod'):
		subdir = "./ProductionDB/"
	else:
		raise AssertionError("No Such DB Instance")
	directory = subdir + "DB_Logs/"
	# print("Printing", updb.inteldb)
	print("TIME RUN STARTED", datetime.datetime.now(), " DIRECTORY", directory)
	last_sync_obj = retrieve_last_sync(directory)
	print("RETURN", last_sync_obj)
	sync_status = updb.update_all_dbs(subdir, last_sync_obj, db_instance_nm)
	print("TIME RUN ENDED", datetime.datetime.now())
	sync_complete_update(sync_status, directory)
	if(not check_sync_response(sync_status)):
		send_sync_error_report(sync_status, directory)


