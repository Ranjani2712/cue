import pymongo #import MongoClient
import json
import bson
import learnosity_sdk
from learnosity_sdk.request import DataApi
import psycopg2
import pandas as pd
from pymongo import UpdateOne
import cuemath_db as cuedb
import config_update_script as cnfg
import math
import datetime
import uuid
import os.path
import os
import sys
import time
from update_from_cuedb import *

default_date = datetime.datetime(2019, 7, 20, 0, 4, 57, 711220)
read_batch_size = cnfg.learnosity_read_batch_size
write_batch_size = cnfg.db_write_batch_size
days_to_subtract = 2
inteldb = None


def parse_activity(input_activity, node_id_list, sheet):
	#Parse each activity into a dictionary object;
	#If version != 1, dt_updated is the new dt_created;
	#Items should have item _ids as well as item_reference; item: items_collection.find_one({'item_reference': item_id}, sort=[( '_id', pymongo.DESCENDING)])
	activity_id, activity_code, activity_description, activity_name, activity_course_type, activity_target, activity_is_live, activity_created_on, activity_updated_on = \
		sheet['id'], sheet['code'], sheet['description'], sheet['name'], sheet['course_type'], sheet['target_accuracy'], sheet['is_live'], sheet['created_on'], sheet['updated_on']
	parsed_activity = {}
	parsed_activity.update(input_activity)
	del parsed_activity['activity_id']
	del parsed_activity['title']
	parsed_activity['activity_itembank_id'] = input_activity['activity_id']
	parsed_activity['_id'] = activity_id
	# parsed_activity['activity_session_id'] = activity_session_id
	parsed_activity['code'] = activity_code
	parsed_activity['created_on'] = activity_created_on
	parsed_activity['updated_on'] = activity_updated_on
	parsed_activity['description_local_db'] = activity_description
	parsed_activity['course_type'] = activity_course_type
	parsed_activity['name'] = activity_name
	parsed_activity['is_live'] = get_boolean_from_string(activity_is_live)
	parsed_activity['target_accuracy'] = float(activity_target)
	existing_activity = inteldb.activity_collection.collection.find_one({'_id':activity_id})
	if(existing_activity is None):
		parsed_activity['nodes_linked'] = node_id_list
		parsed_activity['distribution_score'] = {}
		parsed_activity['distribution_time'] = {}
	else:
		parsed_activity['nodes_linked'] = existing_activity.get('nodes_linked', {}) + node_id_list
		parsed_activity['distribution_score'] =existing_activity.get('distribution_score', {}) 
		parsed_activity['distribution_time'] = existing_activity.get('distribution_time', {})
	print('Activity Saved', activity_code)
	return(parsed_activity)

def check_activity_equivalence(activity_existing, activity_new):
	#Compare Items in activity for equivalence; return true/false for same; 
	return(set(activity_existing['data']['items']) == set(activity_new['data']['items']))

def gen_dict_extract(key, var):
    if hasattr(var,'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result

def get_nodes_dependency_list(criteria):
	criteria_dict = json.loads(criteria)
	dependency_list = list(gen_dict_extract('node_id', criteria_dict))
	return(dependency_list)

def add_node_schema_data(schema_row, tag_data):
	if(schema_row.get('node.course_type') =='PROGRAM'):
		if(schema_row.get('node.is_timed') == 'false' and schema_row.get('node.attempt_location') == 'ATHOME'):
			sheet_type = 'HOMEWORK'
		else:
			if(schema_row.get('node.is_timed') == 'true' or schema_row.get('node.is_timed') == True):
				if(tag_data.shape[0] == 0):
					sheet_type = 'CHAPTER_ASSESSMENT'
				elif(tag_data.shape[0] == 1):
					sheet_type = 'BLOCK_ASSESSMENT'
			else:
				sheet_type = 'LESSON'
		if(tag_data.shape[0] == 1):
			tags = {
				'block_name':tag_data.iloc[0].get('tg.name', None),
				'block_id':tag_data.iloc[0].get('tg.id', None),
				'type':sheet_type,
				'remedial_topic_name': None,
				'remedial_topic_code': None,
				'remedial_type': None
				}
		elif(tag_data.shape[0] == 0):
			tags = {
				'type':sheet_type,
			}
		else:
			print('WS has more than one block tagged')
	elif(schema_row.get('node.course_type') =='REMEDIAL'):
		if(tag_data.shape[0] == 2):
			tags = {
				'type':'REMEDIAL',
				'remedial_topic_name':tag_data[tag_data['tg.type']=='TOPIC'].iloc[0].get('tg.name'),
				'remedial_topic_code':tag_data[tag_data['tg.type']=='TOPIC'].iloc[0].get('tg.code'),
				'remedial_type':tag_data[tag_data['tg.type']=='SHEET'].iloc[0].get('tg.code'),
				'chapter_code':None,
				'chapter_version':None,
				'position_in_chapter':None,
				'block_name':None,
				'block_id':None,
				'program_code':None,
				}
		else:
			print('Remedial does not have 2 tags')
	dependency_list = get_nodes_dependency_list(schema_row.get('node.unlock_criteria'))
	node_obj = {
		'_id':schema_row.get('node.id'),
		'activity_id':schema_row.get('node.worksheet_id'),
		'chapter_code':schema_row.get('chapter.code'),
		'chapter_id':schema_row.get('chapter.id'),
		'chapter_version':schema_row.get('chapter.version'),
		'is_live':get_boolean_from_string(schema_row.get('node.is_live')),
		'is_timed':get_boolean_from_string(schema_row.get('node.is_timed')),
		'attempt_location':schema_row.get('node.attempt_location'),
		'complete_in':schema_row.get('node.complete_in'),
		'complete_criteria':schema_row.get('node.complete_criteria'),
		'reattempt_criteria':schema_row.get('node.reattempt_criteria'),
		'unlock_criteria':schema_row.get('node.unlock_criteria'),
		'position_in_chapter':schema_row.get('node.position'),
		'program_code':schema_row.get('program.code'),
		'dependent_on':dependency_list
	}
	node_obj.update(tags)
	if(schema_row.get('node.course_type') !='REMEDIAL'):
		prg = inteldb.program_collection.collection.find_one({'_id':schema_row.get('program.code')})
		parsed_program = { 
				'grade':schema_row.get('program.grade'), 
				'name':schema_row.get('program.name'), 
				'description':schema_row.get('program.description'), 
				'is_live':get_boolean_from_string(schema_row.get('program.is_live')),
				'prg_id':schema_row.get('program.id'),
				}
		if(prg is None):
			parsed_program.update({'_id':schema_row.get('program.code')})
			inteldb.program_collection.collection.insert_one(parsed_program)
		elif(prg.get('is_live') != get_boolean_from_string(schema_row.get('program.is_live'))):
			inteldb.program_collection.collection.update_one({'_id':prg.get('_id')}, 
					{'$set':parsed_program})

		chp = inteldb.chapter_collection.collection.find_one({'_id':schema_row.get('chapter.code')})
		parsed_chapter = {  
				'position':schema_row.get('chapter.position'), 
				'name':schema_row.get('chapter.name'), 
				'description':schema_row.get('chapter.description'),
				'program_code':schema_row.get('program.code'),
				'is_live':get_boolean_from_string(schema_row.get('chapter.is_live'))
				}
		if(chp is None):
			parsed_chapter.update({'_id':schema_row.get('chapter.code')})
			inteldb.chapter_collection.collection.insert_one(parsed_chapter)
		elif(chp.get('is_live')==False and get_boolean_from_string(schema_row.get('chapter.is_live'))):
			inteldb.chapter_collection.collection.update_one({'_id':chp.get('_id')}, 
					{'$set':parsed_chapter})
	# print("add_node_schema_data", schema_row['node.worksheet_id'], node_obj['type'])
	return(node_obj)	

def set_inteldb(instance_str):
	global inteldb
	inteldb = IntelDB(instance_str)

def update_activities(directory, df_activities, df_node_schema, df_tags, last_sync_timestamp=default_date, manual_enter = False):
	# Collects all changes to be done in our Activity Collection (On our Mongo Side)
	# TODO: Insert Node based data onto activities.
	print("Starting Activity Collection")
	activity_ref_list_full = []
	activity_update_objs=[]
	node_update_objs = []
	activity_incorrect_objs = []
	fl_nm_reinsert_activities = directory + "DB_Insert_Issues/Activities/db_to_reinsert_"+ str(datetime.date.today()).replace('-','_') + ".json"
	act_idx = 0
	worksheets_updated = df_activities[df_activities['updated_on'] > pd.Timestamp(last_sync_timestamp)]
	all_activities_cursor =  inteldb.activity_collection.collection.find({'_id':{'$in':list(worksheets_updated['id'])}})
	print('updated_worksheets: ', worksheets_updated.shape[0], df_activities.shape[0])
	all_activities = {}
	for indx, actvt in enumerate(all_activities_cursor):
		all_activities[actvt.get('_id')] = actvt
	for idx, sheet in worksheets_updated.iterrows():
		stored_activity = all_activities.get(sheet['id'])
		if(stored_activity is None):
			activity_ref_list_full.append(sheet['learnosity_activity_ref'])
		else:
			if(stored_activity.get('reference') != sheet['learnosity_activity_ref']):
				arch_refs = stored_activity.get('archived_references', []).append(stored_activity.get('reference'))
				newVals = {'reference':sheet['learnosity_activity_ref'],
							'archived_references':arch_refs
							}
			if(sheet['updated_on'] != stored_activity.get('updated_on')):
				newVals = {
					'code':sheet['code'],
					'updated_on':sheet['updated_on'],
					'is_live':sheet['is_live'],
					'name':sheet['name'],
					'description_local_db':sheet['description'],
					'course_type':sheet['course_type'],
				}
			activity_update_objs.append(UpdateOne({'_id':stored_activity.get('_id')}, {'$set':newVals}))
	nodes_updated = df_node_schema[df_node_schema['node.updated_on'] > pd.Timestamp(last_sync_timestamp)]
	all_node_activities_cursor = inteldb.activity_collection.collection.find({'_id':{'$in':list(nodes_updated['node.worksheet_id'])}})
	print('updated_nodes: ', nodes_updated.shape[0], df_node_schema.shape[0])
	all_nd_activities = {}
	for nd_actvt in all_node_activities_cursor:
		all_nd_activities[nd_actvt.get('_id')] = nd_actvt
	for idx, schema_row in nodes_updated.iterrows():
		tag_data = df_tags[(df_tags['ent.entity_type']=='NODE') & (df_tags['ent.entity_id']==schema_row['node.id'])]
		node_obj = add_node_schema_data(schema_row, tag_data)
		node_update_objs.append(UpdateOne({'_id':node_obj['_id']}, {'$set':node_obj}, upsert=True))
		stored_activity = all_nd_activities.get(schema_row['node.worksheet_id'], None)
		if(stored_activity is not None):
			node_set = stored_activity['nodes_linked']
			node_set.append(node_obj['_id'])
			node_set = list(set(node_set))
			activity_update_objs.append(UpdateOne({'_id':stored_activity['_id']}, {'$set':{'nodes_linked':node_set}}, upsert=True))
	for j in range(1, int(math.ceil(len(activity_ref_list_full)/read_batch_size))+1):
		print("Creating Activity Request")

		activity_ref_list=activity_ref_list_full[(read_batch_size*(j-1)):(read_batch_size*j)]
		data_request_activity = {
		"sort": "asc",
		"mintime": str(last_sync_timestamp),
		"sort_field":"updated",
		"include":{
			"activities":["dt_created", "dt_updated", "last_updated_by", "activity_id"]
			},
		"references": list(set(activity_ref_list))
		}
		with open(directory + "activity_learnosity_request.json", "w") as fp:
			json.dump(data_request_activity, fp, indent = 4, default = str)
		client_learnosity = DataApi()
		res = client_learnosity.results_iter(cnfg.endpoint_activities, cnfg.security, cnfg.consumer_secret, data_request_activity, cnfg.action)
		for activity in res:
			# Check each activity and look for it in client_mongo; If found, check item set. If same, overwrite last_updated_on; If not, parse activity, with version number, and dt_created
			print('Creaating ', activity.get('reference'))
			try:
				worksheet_by_ref = df_activities[df_activities['learnosity_activity_ref']==activity['reference']]
				if(worksheet_by_ref.shape[0]==0):
						# continue
					raise Exception("Spooky Things are happening. No Activity ID exists on our Side, given this activity reference:", activity['reference'])
				else:
					for idx, activity_id in enumerate(list(worksheet_by_ref['id'])):
						act_idx += 1
						# print("worksheet #", act_idx, activity_id)
						sheet = worksheet_by_ref.iloc[idx]
						node_id_list = []
						schema_rows = df_node_schema[df_node_schema['node.worksheet_id'] == activity_id]
						if(schema_rows.shape[0] != 0):
							for idx, schema_row in schema_rows.iterrows():
								tag_data = df_tags[(df_tags['ent.entity_type']=='NODE') & (df_tags['ent.entity_id']==schema_row['node.id'])]
								node_obj = add_node_schema_data(schema_row, tag_data)
								node_update_objs.append(UpdateOne({'_id':node_obj['_id']}, {'$set':node_obj}, upsert=True))
								node_id_list.append(node_obj['_id'])
						else:
							# if(sheet['is_live']=='true'):
							print(activity['reference'] + " not in Schema; Activity ID = ", activity_id)
							# continue
						# total_worksheets_num+=1
						parsed_activity = parse_activity(activity, node_id_list, sheet)
						activity_update_objs.append(UpdateOne({'_id':sheet['id']}, {'$set':parsed_activity}, upsert=True))
			except Exception as e:
				activity['parse_error'] = e
				print("Exception ", e)
				activity_incorrect_objs.append(activity)
			# Parse Each Result that already did not exist using parse_activity_data(df, activity): 1 is the version number
	print("TOTAL WORKSHEETS: ", len(activity_update_objs), len(activity_incorrect_objs), len(node_update_objs), act_idx, fl_nm_reinsert_activities)
	with open(fl_nm_reinsert_activities, "w") as fp:
		json.dump({'Failed_Activities':activity_incorrect_objs}, fp, indent = 4, default=str)
	return(activity_update_objs, node_update_objs)

def parse_item(item):
	#Parse Item into item and questions
	parsed_item={}
	parsed_item.update(item)
	parsed_item['_id'] = item.get('reference').strip() #generated_uuid_for_id(inteldb.item_collection)
	parsed_item['distribution_score'] = {}
	parsed_item['distribution_time'] = {}
	return(parsed_item)

def update_items(directory, df_activities, last_sync_timestamp=default_date):
	# Collect all newly created/updated items from learnosity. In case of update, check for versioning change.
	item_updates=[]
	fl_nm_reinsert_items = directory + "DB_Insert_Issues/Items/db_to_reinsert_"+ str(datetime.date.today()).replace('-','_') + ".json"
	#question_objs=[]
	items_incorrect = []
	data_request_items = {
		"sort": "asc",
		"sort_field":"updated",
		"mintime": str(last_sync_timestamp),
		"include":{
			"items":["dt_created", "dt_updated", "last_updated_by"]
			},
		"status":["published"]
	}
	client_learnosity = DataApi()
	res = client_learnosity.results_iter(cnfg.endpoint_items, cnfg.security, cnfg.consumer_secret, data_request_items, cnfg.action)
	for idx, item in enumerate(res):
		#Check each item and look for it in client_mongo; If found, check meta_field. If same, overwrite object; If not, parse item, with new version number and dt_created
		#If item did not exist, parse object using parse_item_data(item, 1): 1 is version number
		print("Item #", idx)
		#try:
			# num_items_existing = inteldb.item_collection.collection.count_documents({'reference': item['reference']})
		item_obj = parse_item(item)
		item_updates.append(UpdateOne({'_id':item_obj['_id']}, {'$set':item_obj}, upsert = True))
		#question_objs+=question_parsed
		#except Exception as e:
		#	item['parse_error'] = e
		#	items_incorrect.append(item)
	with open(fl_nm_reinsert_items, "w") as fp:
		json.dump({'Failed_Items':items_incorrect}, fp, indent = 4, default=str)
	return(item_updates)

def generated_uuid_for_id(tbl_collection):	
	# Generate UUID for item_response; Check if item_responses.collection.find_one({'_id':generated_uuid}) returns None
	uu = uuid.uuid4()
	uu = str(uu)
	uu.replace(".", "")
	uu.replace("$", "")
	chk = tbl_collection.collection.find_one({'_id':uu})
	if(chk is not None):
		uu = generated_uuid_for_id(tbl_collection)
	else:
		return(uu)

def calculate_cuescore(score, attempts):
	try:
		cuescore = float(score/attempts)
	except TypeError:
		try:
			cuescore = float(score)
		except TypeError:
			print(score, attempts)
			cuescore = 0.0
	if(cuescore is None):
		return(score)
	return(cuescore)

def check_user_authenticity(inteldb, df_student_attempts, user_id, worksheet_id, session_id):
	'''
	Returns whether or not the session is to be taken as authentic. 
	# 
	# CHECKs and only update sessions approved by our tech; Check and filter out Demo Students here
	'''
	student_stored = inteldb.student_collection.collection.find_one({'_id':user_id})
	if(student_stored is None):
		print("DEBUG_CHECK_AUTH_NO_STUDENT_FOUND: ", user_id)
		return(False)
	student_attempt_rows = df_student_attempts[(df_student_attempts['user_id']==user_id) & (df_student_attempts['worksheet_id']==worksheet_id)]
	if(student_attempt_rows.shape[0] == 0):
		# student_attempt_rows = IntelDB.get_student_attempts_data_by_attempt_id(session_id)
		# if(student_attempt_rows.shape[0] == 0):
		print("NOT IN attempt_db DB: user", user_id, "activity", worksheet_id, "session_id", session_id)
		return(False)
		#raise Exception("Student Attempt not in DB")
	else:
		if(student_attempt_rows.iloc[0].get('completed_on') and pd.isnull(student_attempt_rows.iloc[0]['completed_on'])):
			# print("YAHAAAAAAAN", type(student_attempt_rows.iloc[0]['started_on']))
			if(type(student_attempt_rows.iloc[0]['started_on'])==type('hello world')):
				dt_cmp = datetime.datetime.strptime(student_attempt_rows.iloc[0]['started_on'], "%Y-%m-%d %H:%M:%S.%f")
			else:
				dt_cmp = student_attempt_rows.iloc[0]['started_on']
			if(dt_cmp>(datetime.datetime.now() - datetime.timedelta(days=20))):
				return(not student_stored['is_demo'])
			else:
				return False
		else:
			return(not student_stored['is_demo'])
		
def parse_sessions(inteldb, session, node_linked, student_node_row, student_attempt_row, item_updates, activity_updates):
	# Parse Sessions into item_responses and session objects: Based on Timestamp etc. test well.
	parsed_session = {}
	item_responses_errors = []
	parsed_item_responses = []
	parsed_session.update(session)
	#Changing Session_id to _id on db
	parsed_session['_id'] = session['session_id']
	del parsed_session['session_id']
	del parsed_session['responses']
	del parsed_session['items']
	activity_linked = inteldb.activity_collection.collection.find_one({'_id':session['activity_id']})
	items_linked = inteldb.item_collection.collection.find({'_id':{'$in':[item.get('reference').strip() for item in session.get('items', [])]}})
	items_linked_dict = {}
	for item_obj in items_linked:
		items_linked_dict[item_obj.get('_id')] = item_obj
	if(activity_linked):
		parsed_session['activity_id'] = activity_linked['_id']
	else:
		# parsed_session['activity_id'] = node_linked['activity_id']
		# parsed_session['activity_reference'] = activity_linked['reference']
		print("KEYERROR", session['session_id'], session['activity_id'])
		raise(KeyError('No Such Activity Found'))
		# import pdb; pdb.set_trace()
	parsed_session['item_responses'] = []
	parsed_session['cuescore_available'] = True
	session_cuescore, session_time, session_total_attempts = 0.0, 0.0, 0.0
	response_id_list = [response['response_id'] for response in session['responses']]
	item_response_time_dict = {}
	total_num_responses_attempted = 0
	for item in session['items']:
		# Get _id from items according to timestamp, search by reference; Update avg_score, avg_time, std_dev_score, std_dev_time, total_num_responses for item_linked
		# Get Responses; Calculate cuescores, time_total on the basis of metadata: attempts
		item_linked = items_linked_dict.get(item.get('reference'))
		if(item_linked is not None):
			item_response = dict()
			item_response['activity_id'] = parsed_session['activity_id']	
			item_response['item_id'] = item_linked['_id']	
		else:
			print("ITEM not found", item['reference'], activity_linked['_id'])
			continue
			# raise Exception("EXCEPTION_KEYERROR_ITEM_ACTIVITY", item_linked, activity_linked, item['reference'], dt_response_last_updated)
		item_response['cuescore_available']=True
		item_cuescore = 0.0
		item_max_score = 0.0
		item_score = 0.0
		item_total_attempts = 0
		item_num_attempts = 0
		responses_in_item = []
		response_dts = []
		response_time_dict = {}
		num_responses_attempted = 0
		for response_id in item['response_ids']:
			response_index = response_id_list.index(response_id)
			response_obj = session['responses'][response_index]
			response_dts.append(response_obj['dt_score_updated'])
			#What to do with attempt data?
			try:
				if(response_obj['attempted']):
					num_responses_attempted += 1
					#if(response_obj['response'] is not None):
					try:
						#print("!", response_obj['score'], response_obj['response']['metadata']['attempts'])
						item_cuescore += calculate_cuescore(response_obj['score'], response_obj['response']['metadata']['attempts'])
						if(response_obj['score'] is None):
							response_obj['score'] = 0
						if(response_obj['max_score'] is None):
							response_obj['max_score'] = 0
						#if(response_obj['response']['metadata']['attempts'] is None):
						#	response_obj['response']['metadata']['attempts'] = 0
						try:
							item_response['events'] = response_obj['response']['metadata']['events']
						except KeyError:
							item_response['events'] = None
						item_total_attempts += response_obj['response']['metadata']['attempts']
						response_obj['cuescore_available'] = True
						item_num_attempts += 1
					except KeyError:
						try:
							item_cuescore += response_obj['score']
						except TypeError:
							pass
						response_obj['cuescore_available'] = False
						parsed_session['cuescore_available'] = False
						item_response['cuescore_available'] = False
						item_total_attempts += 1
					try:
						item_score += response_obj['score']
					except TypeError:
						pass
				else:
					item_score = item_score
			except TypeError as e:
				#print("TypeError once more : ", e)
				#print("Session_ID:", session['session_id'])
				response_obj['cuescore_available'] = False
				parsed_session['cuescore_available'] = False
				item_response['cuescore_available'] = False
				raise(TypeError(session['session_id'] + " could not be inserted as " + str(e)))
				#import pdb; pdb.set_trace()
			try:
				#import pdb; pdb.set_trace()
				#CHECK FOR ANY OTHER INCONSISTENCIES HERE
				v = response_obj['response']['value']['.']
				response_obj['response']['value']['x'] = v
				del response_obj['response']['value']['.']
				print(". key deleted")
			except KeyError:
				pass
			except TypeError:
				pass
			#try:
			if(response_obj.get('max_score', None)):
				try:
					item_max_score += response_obj.get('max_score', 0)
				except TypeError:
					import pdb; pdb.set_trace()
					continue
					# datetime.datetime.strptime(v_ques, "%Y-%m-%dT%H:%M:%SZ")
			responses_in_item.append(response_obj)
			try:
				dt_tm_response = datetime.datetime.strptime(response_obj['dt_score_updated'], "%Y-%m-%d %H:%M:%S")
			except ValueError:
				dt_tm_response = datetime.datetime.strptime(response_obj['dt_score_updated'], "%Y-%m-%dT%H:%M:%SZ")
			response_time_dict[response_obj['question_reference']] = str(dt_tm_response + datetime.timedelta(hours =5, minutes= 30))

		dt_response_last_updated = max(response_dts)
		# print('Item_linked', item_linked)
		item_response.update(item)
		del item_response['response_ids']
		# Generate _id for item_response; Save to parsed_session['item_responses'] as well.
		item_response['_id'] = session['session_id'] + "_" + item['reference']
		item_response_time_dict[item_response['_id']] = {}
		item_response_time_dict[item_response['_id']]['questions'] = response_time_dict
		item_response_time_dict[item_response['_id']]['time'] = item['time']
		item_response_time_dict[item_response['_id']]['score'] = item_score
		item_response_time_dict[item_response['_id']]['cuescore'] = item_cuescore
		item_response_time_dict[item_response['_id']]['max_score'] = item_max_score
		item_response_time_dict[item_response['_id']]['num_attempts'] = item_total_attempts
		item_response_time_dict[item_response['_id']]['num_questions'] = num_responses_attempted
		item_response['activity_id'] = parsed_session['activity_id']
		item_response['user_id'] = parsed_session['user_id']
		item_response['responses'] = responses_in_item
		item_response['total_attempts'] = item_total_attempts
		item_response['num_attempted'] = item_num_attempts
		item_response['score'] = item_score
		item_response['max_score'] = item_max_score
		item_response['cuescore'] = item_cuescore
		newvalues_item = {"$set": {"distribution_score."+session['user_id']:item_score, "distribution_time."+item_response['user_id']:item['time'], 'max_score': item_max_score}}
		item_updates.append(UpdateOne({'_id': item_linked['_id']}, newvalues_item, upsert = True))
		session_time += item['time']
		session_cuescore += item_cuescore
		session_total_attempts += item_total_attempts
		total_num_responses_attempted += num_responses_attempted
		parsed_item_responses.append(item_response)
	parsed_session['cuescore'] = session_cuescore
	parsed_session['time_total'] = session_time
	parsed_session['total_attempts'] = session_total_attempts
	parsed_session['avg_attempts'] = session_total_attempts/parsed_session['num_questions']
	# score_distr[session['user_id']] = session['score']
	newvalues_activity = {"$set": {"distribution_score."+session['user_id']:session['score'], "distribution_time."+session['user_id']:session_time}}
	if(not activity_linked.get("max_score")):
		newvalues_activity['$set']["max_score"] = session.get("max_score")
	activity_updates.append(UpdateOne({'_id': activity_linked['_id']}, newvalues_activity))
	# print("parse_sessions adding activity data", newvalues_activity)
	date_session_dict = cluster_items_by_dates(item_response_time_dict, parsed_session['num_questions'])
	parsed_session['item_response_breakdown'] = date_session_dict
	parsed_session['attempt_finished'] = (total_num_responses_attempted == session['num_questions'])
	# if(get_boolean_from_string(student_node_row['unlock_status'])):
		# df_student_nodes
	parsed_session['dt_assigned'] = str(student_node_row['created_on'])
	try:
		dt_started = datetime.datetime.strptime(parsed_session['dt_started'], "%Y-%m-%d %H:%M:%S")
		dt_completed = datetime.datetime.strptime(parsed_session['dt_completed'], "%Y-%m-%d %H:%M:%S")
	except ValueError:
		dt_started = datetime.datetime.strptime(parsed_session['dt_started'], "%Y-%m-%dT%H:%M:%SZ")
		dt_completed = datetime.datetime.strptime(parsed_session['dt_completed'], "%Y-%m-%dT%H:%M:%SZ")
	parsed_session['dt_started']=dt_started + datetime.timedelta(hours =5, minutes= 30)
	parsed_session['dt_completed']=dt_completed + datetime.timedelta(hours =5, minutes= 30)
	parsed_session['dt_due'] = str(student_node_row['created_on'] + datetime.timedelta(days=node_linked['complete_in']))
	parsed_session['attempt_location'] = node_linked['attempt_location']
	parsed_session['is_timed'] = node_linked['is_timed']
	parsed_session['node_id'] = node_linked['_id']
	parsed_session['activity_type'] = node_linked['type']
	parsed_session['points_earned'] = student_node_row['points_earned']
	parsed_session['activity_chapter_code'] = node_linked['chapter_code']
	parsed_session['activity_chapter_version'] = node_linked['chapter_version']
	parsed_session['completed_on'] = str(student_attempt_row['completed_on'])
	parsed_session['activity_reference'] = student_attempt_row.get('learnosity_activity_ref')
	parsed_session['session_cleared'] = session_cleared_status(student_attempt_row, student_node_row)
	parsed_session['completion'] = (float(total_num_responses_attempted)/parsed_session['num_questions'])
	# print("DEBUG_FETCH", item_response_time_dict, date_session_dict)
	return(parsed_session, parsed_item_responses, date_session_dict, item_updates, activity_updates)

def session_cleared_status(student_attempt_row, student_node_row):
	# Cleared, Not_Cleared, In_Progress
	if(pd.isnull(student_attempt_row['completed_on'])):
		return('In_Progress')
	else:
		if(student_node_row['complete_status'] == 'true' and student_node_row['last_attempt_id'] == student_attempt_row['id']):
			return('Cleared')
		else:
			return('Not_Cleared')

def get_boolean_from_string(val):
	if(val is None or pd.isnull(val)):
		return(False)
	if(val.strip() == 'true'):
		return(True)
	elif(val.strip() == 'false'):
		return(False)

def cluster_items_by_dates(item_response_time_dict, session_total_questions):
	'''
	For now we cluster sessions by date
	'''
	date_dict = {}
	total_num_questions = 0
	for item_response_id, item_response_obj in item_response_time_dict.items():
		#print("V_item", v_item)
		num_keys = 0
		for response_id, response_dt_time in item_response_obj['questions'].items():
			num_keys += 1
			try:
				## TODO: Introduce Splitting here in case of changing logic from per day session to within day.
				dt = response_dt_time.split(" ")[0]
				dt = dt.strip()
				# print("cluster_items_by_dates", response_dt_time, dt)
				date_dict[dt]['item_response_dict'][item_response_id].update({response_id:response_dt_time})
				if(response_dt_time < date_dict[dt]['dt_started']):
					date_dict[dt]['dt_started'] = response_dt_time
				if(response_dt_time > date_dict[dt]['dt_completed']):
					date_dict[dt]['dt_completed'] = response_dt_time
			except KeyError:
				try:
					date_dict[dt]['item_response_dict'].update({item_response_id:{response_id:response_dt_time}})
					if(response_dt_time < date_dict[dt]['dt_started']):
						date_dict[dt]['dt_started'] = response_dt_time
					if(response_dt_time > date_dict[dt]['dt_completed']):
						date_dict[dt]['dt_completed'] = response_dt_time
				except KeyError:
					date_dict[dt] = {'item_response_dict':{item_response_id:{response_id:response_dt_time}}, \
									'time_spent':0, \
									'score':0, \
									'cuescore':0, \
									'max_score':0,\
									'num_attempts':0,\
									'dt_started':response_dt_time,\
									'dt_completed':response_dt_time,\
									'session_total_questions':session_total_questions,\
									'num_attempted_today':0,
									}

		date_dict[dt]['time_spent'] += item_response_obj['time']
		date_dict[dt]['score'] += item_response_obj['score']
		date_dict[dt]['cuescore'] += item_response_obj['cuescore']
		date_dict[dt]['max_score'] += item_response_obj['max_score']
		date_dict[dt]['num_attempts'] += item_response_obj['num_attempts']
		# total_num_questions += num_keys
		date_dict[dt]['num_attempted_today'] += num_keys
		# date_dict[dt]['total_num_questions'] += item_response_obj['num_questions']
		#date_dict[dt][]
	cumulative_ques = 0
	for k in sorted(date_dict.keys()):
		cumulative_ques += date_dict[k]['num_attempted_today']
		date_dict[k]['num_attempted_cumulative'] = cumulative_ques
	#if(len(date_dict.keys())>0):
	#	print("DEBUG_DATE_DICT: ", date_dict)
	return(date_dict)

def get_av_data_for_user(av_cache_dt, user_id, teacher_id):
	df_av_monitor, df_av_request, df_av_whiteboard = av_cache_dt
	#print(df_av_monitor, df_av_request, df_av_whiteboard)
	#print(type(df_av_monitor["user_id"]), type(df_av_request['requested_by']), type(df_av_whiteboard['user_id']))
	if(df_av_monitor.shape[0] != 0):
		# Find a way to include teacher
		df_av_monitor_user = df_av_monitor[((df_av_monitor["user_id"]==teacher_id) & (df_av_monitor["user_role"]=='moderator'))\
											| ((df_av_monitor["user_id"]==user_id) & (df_av_monitor["user_role"]=='publisher'))]
		student_sessions_possible = list(df_av_monitor[((df_av_monitor["user_id"]==user_id) & (df_av_monitor["user_role"]=='publisher'))]['session_id'])
		df_av_monitor_user = df_av_monitor_user[df_av_monitor_user['session_id'].isin(student_sessions_possible)]
		# print("DEBUG_get_av_data_for_user: ", df_av_monitor_user.loc[:, ('session_id', 'user_id', 'user_role')])
		#print(df_av_monitor_user)
		#print(set(df_av_monitor_user['user_id']), "session_ids:", set(df_av_monitor_user['session_id']))
	else:
		df_av_monitor_user = df_av_monitor
	if(df_av_request.shape[0] != 0):
		df_av_request_user = df_av_request[df_av_request['requested_by']==user_id]
	else:
		df_av_request_user = df_av_request
	if(df_av_whiteboard.shape[0] != 0):
		df_av_whiteboard_user = df_av_whiteboard[df_av_whiteboard["user_id"]==user_id]
	else:
		df_av_whiteboard_user = df_av_whiteboard
	return(df_av_monitor_user, df_av_request_user, df_av_whiteboard_user)

def populate_student_side(inteldb, node_linked, session, av_cache, classroom_updates, student_updates):
	'''
	TODO: Make Updates bulk objects.
	Populates Classroom as well as Homework and Assessment data.
	Homework/Remedial/Assessment : append to homework/assessment/remedial in student:
	{
	<activity_id>:
		{
		<session_id>:
			{
			dt_completed: (NULL if incomplete),
			activity_cleared: True/False,
			percent_completed: ?,
		}
	}
	av_data:{
		[doubts_on:(item_response_id, question_id, num_button_press)], 
		[calls:{duration, connection_lag, meta:{img_fl_links, video_link}}]}}
	'''
	user_id, activity_id = session['user_id'], session['activity_id']
	toggle_remedial = False
	student = inteldb.student_collection.collection.find_one({'_id':user_id})
	activity_type = node_linked.get('type')
	# teacher = teacher_collection.collection.find_one({'_id':student['teacher_id']})
	newvals_student = {"$set":{}}
	newvals_teacher = {"$set":{}}
	session_meta_obj = {}
	date_list = []
	for idx, dte in enumerate(session['item_response_breakdown'].keys()):
		date_list.append(dte)
	classrooms_cursor = inteldb.classroom_collection.collection.find({'student_id':user_id, 'date':{'$in':date_list}})
	all_classrooms_session = {}
	for idx, clsrm in enumerate(classrooms_cursor):
		all_classrooms_session[clsrm.get('date')] = clsrm
	for idx, date in enumerate(session['item_response_breakdown'].keys()):
		# Find Appropriate Classroom and Link all data there
		completion_session = (float(session['item_response_breakdown'][date]['num_attempted_cumulative'])/session['item_response_breakdown'][date]['session_total_questions'])
		session_cleared = ((completion_session == 1) and session['session_cleared']=='Cleared')
		session_meta_obj.update({
						date:{
						'activity_id':session['activity_id'],
						'activity_reference':session['activity_reference'],
						'node_id':node_linked.get('_id'),
						'session_cleared':session_cleared,
						'completion':completion_session,
						'session_total_questions':session['item_response_breakdown'][date]['session_total_questions'],
						'num_attempted_today':session['item_response_breakdown'][date]['num_attempted_today'],
						'num_attempted_cumulative':session['item_response_breakdown'][date]['num_attempted_cumulative'],
						'score':session['item_response_breakdown'][date]['score'],
						'time_spent':session['item_response_breakdown'][date]['time_spent'],
						'max_score':session['item_response_breakdown'][date]['max_score'],
						'dt_started':session['item_response_breakdown'][date]['dt_started'],
						'dt_completed':session['item_response_breakdown'][date]['dt_completed'],
						}})
		if(node_linked.get('attempt_location')!="ATHOME" and all_classrooms_session.get(date) is not None):
			#print("DEBUG HERE", date_session_dict.keys(), activity_type)
			#newvals_student["$set"] = {'classwork':student['classwork']}
			#newvals_teacher["$set"] = {'classwork':teacher['classwork']}
			#Does not work if student has > 1 class in a day: Different teachers for example
			# print("DEBUG HERE", activity_type, session_meta_obj)
			# classroom = inteldb.classroom_collection.collection.find_one({'student_id':user_id, 'date':date})
			classroom = all_classrooms_session.get(date)
			if(classroom is None):
				print("DEBUG_CLASSROOM_NONE: ", session['_id'], user_id, date)
			sessions_in_class = classroom['sessions_attempted']
			#TODO: Make Session Meta Object: Completion %, Cleared, Num of Questions-Total, Num Attempted; Today Plus cumulative.
			# NOTE that meta object here contains info on Session ONLY for today's class.`
			session_meta_obj_dt = session_meta_obj[date]
			try:
				sessions_in_class.update({session['_id']:session_meta_obj_dt})
			except KeyError:
				sessions_in_class = {session['_id']:session_meta_obj_dt}
			except AttributeError:
				sessions_in_class = {session['_id']:session_meta_obj_dt}
			#try:
			# tm_strt = datetime.datetime.now()
			try:
				av_cache_user= get_av_data_for_user(av_cache[date], user_id, classroom['teacher_id'])
			except KeyError:
				av_cache[date] = IntelDB.get_av_data_by_date(date)
				av_cache_user = get_av_data_for_user(av_cache[date], user_id, classroom['teacher_id'])
			# class_strt_dt_time = datetime.datetime.combine(classroom['date'], classroom[''])
			av_data = parse_av_data(av_cache_user, user_id,
								classroom['teacher_id'], session['item_response_breakdown'][date], 
								session['_id'], date, classroom.get('enter_time', 
								classroom.get('scheduled_start_time')))
			# print("DEBUG_AV_DATA", {'av_data':av_data})
			# tm_end = datetime.datetime.now()
			# newvals = {"$set":{'sessions_attempted':sessions_in_class, 'av_data':av_data}}
			newvals = {"$set":{'sessions_attempted.'+session['_id']:session_meta_obj_dt, 'av_data':av_data}}
			# print("Updating Classrooms", newvals);
			classroom_updates.append(UpdateOne({'_id': classroom['_id']}, newvals))
			# classroom_collection.collection.update_one({'_id':classroom['_id']}, newvals)
		# print("DEBUG", "No Classroom")
	student_session_cleared = (session['session_cleared']=='Cleared')
	student_session_meta_obj = {\
						'activity_id':session['activity_id'],
						'activity_reference':session['activity_reference'],
						'node_id':node_linked.get('_id'),
						'session_cleared':student_session_cleared,
						'completion':session['completion'],
						'session_total_questions':session['num_questions'],
						'num_attempted':session['num_attempted'],
						'score':session['score'],
						'time_spent':session['time_total'],
						'max_score':session['max_score'],
						'dt_started':session['dt_started'],
						'dt_completed':session['completed_on'],
						'dt_assigned':session['dt_assigned'],
						# 'dt_due':session['dt_due'],\
						}
	# print('DEBUG_POPULATE_STUDENT', student_session_meta_obj)
	if(activity_type=="LESSON"):
		student_meta_obj_key = 'lessons'
		#TODO: Make Session Meta Object: Completion %, Cleared, Num of Questions-Total, Num Attempted; Today Plus cumulative.
	if(activity_type=="HOMEWORK"):
		student_meta_obj_key = 'homework'			#Used to determine what object within student do we append object to
			#raise Exception("Homework Neither Valid nor Invalid: ", session_id)
	elif(activity_type=="BLOCK_ASSESSMENT" or activity_type=='CHAPTER_ASSESSMENT'):
		student_meta_obj_key = 'assessments'
	elif(activity_type=="REMEDIAL"):
		student_meta_obj_key = 'remedials'
	# newvals_student["$set"].update({student_meta_obj_key: get_meta_obj})
	newvals_student["$set"].update({student_meta_obj_key+"."+activity_id+".sessions_attempted."+session.get('_id'): student_session_meta_obj})
	if(newvals_student["$set"]!={}):
		student_updates.append(UpdateOne({'_id': user_id}, newvals_student))
		# student_collection.collection.update_one({'_id':user_id}, newvals_student)
	#if(newvals_teacher["$set"]!={}):
	#	teacher_updates.append(UpdateOne({'_id': student["teacher_id"]}, newvals_teacher))
		# teacher_collection.collection.update_one({'_id':student["teacher_id"]}, newvals_teacher)
	return(av_cache, classroom_updates, student_updates)

def retrieve_learnosity_session_data(inteldb, directory, df_activities, df_student_nodes, df_student_attempts, df_node_schema, last_sync_timestamp=default_date):
	'''
	# Given all the activities from before, (retrieve from Mongo) get any sessions. Return parsed sessions data
	# Also, update the students collection with newly inserted sessions. (Also filter out demo sessions.)
	'''
	fl_nm_reinsert_sessions = directory + "DB_Insert_Issues/Sessions/db_to_reinsert_"+ str(datetime.date.today()).replace('-','_') + ".json"
	activity_ref_list_full = list(df_activities['id'])
	session_start_sync_dt = datetime.datetime.now()
	#print("DEBUG", activity_ref_list_full)
	session_objs, session_updates = [], []
	item_updates, activity_updates, classroom_updates, student_updates = [], [], [], []
	item_response_objs, item_response_updates = [], []
	session_incorrect_objs = []
	#todo channge
	for j in range(1, int(math.ceil(len(activity_ref_list_full)/read_batch_size))+1):
		activity_id_list=activity_ref_list_full[(read_batch_size*(j-1)):(read_batch_size*j)]
		data_request_session = {
			"activity_id": activity_id_list,
			"status": [
				"Completed"
				],
			"sort": "asc",
			"mintime": str(last_sync_timestamp),
			"maxtime": str(session_start_sync_dt),
		}
		client_learnosity = DataApi()
		res = client_learnosity.results_iter(cnfg.endpoint_sessions, cnfg.security, cnfg.consumer_secret, data_request_session, cnfg.action)
		av_cache = {}
		session_num = 0
		for idx, session in enumerate(res):
			if(len(item_updates) >= write_batch_size):
				print("UPDATING ITEMS")
				insert_gracefully(inteldb.item_collection, item_updates, ordered=True)
				item_updates = []
			if(len(activity_updates) >= write_batch_size):
				print("UPDATED ACTIVITIES:")
				insert_gracefully(inteldb.activity_collection, activity_updates, ordered=True)
				activity_updates = []
			if(len(classroom_updates) >= write_batch_size):
				print("UPDATED CLASSROOMS:")
				insert_gracefully(inteldb.classroom_collection, classroom_updates, ordered=True)
				classroom_updates = []
			if(len(student_updates) >= write_batch_size):
				print("UPDATED STUDENTS:")
				insert_gracefully(inteldb.student_collection, student_updates, ordered=True)
				student_updates = []
			try:
				check = check_user_authenticity(inteldb, df_student_attempts, session['user_id'], session['activity_id'], session['session_id'])
				print("Session Number:", idx, len(item_updates), len(activity_updates), check)
				if(check):
				# Above Statement Checks for dummy users and does not consider their responses
					try:
						# TODO: Get Other data: Assignment, Points Earned, Completed, pass criteria, valid_criteria
						student_node_row = df_student_nodes[(df_student_nodes['worksheet_id'] == session['activity_id']) & (df_student_nodes['user_id'] == session['user_id'])]
						student_attempt_row = df_student_attempts[df_student_attempts['id'] == session['session_id']]
						#HOW TO GET NODE_ID
						node_id_set = set(list(student_node_row['node_id']))
						node_linked = None
						if(len(node_id_set) == 1):
							node_id = node_id_set.pop()
							node_linked = inteldb.node_collection.collection.find_one({'_id':node_id})
						else:
							nodes_linked_options = inteldb.node_collection.collection.find({'_id':{'$in':list(node_id_set)}})
							for nd in nodes_linked_options:
								if(nd.get('activity_id')==session['activity_id']):
									node_linked = nd
						if(node_linked is None):
							print("KeyError: NODE NOT FOUND", session['session_id'], session['dt_started'], session['activity_id'], node_id_set)
						if(node_linked is not None and student_node_row.shape[0] == 1 and student_attempt_row.shape[0] != 0):
							session_parsed, item_responses_parsed, date_session_dict, item_updates, activity_updates = \
								parse_sessions(inteldb, session, node_linked, student_node_row.to_dict(orient='records')[0], student_attempt_row.to_dict(orient='records')[0], item_updates, activity_updates)
							session_updates.append(UpdateOne({'_id': session_parsed['_id']}, {'$set': session_parsed}, upsert = True))
							item_response_updates += [UpdateOne({'_id': item_response['_id']}, {'$set': item_response}, upsert = True) for item_response in item_responses_parsed]
							av_cache, classroom_updates, student_updates = populate_student_side(inteldb, node_linked, session_parsed, av_cache, classroom_updates, student_updates)
							if(len(session_updates) > cnfg.bulk_write_batch_size):
								print("Writing Data so far 1:", len(session_updates), len(item_response_updates))
								yield(session_updates, item_response_updates, -1)
								session_updates, item_response_updates = [], []
								print("Writing Data so far 2:", len(session_updates), len(item_response_updates))
					except IndexError:
						#print(session)
						print("INDEX_ERROR", session['user_id'], session['activity_id'], session['session_id'])
			except Exception as e:
				session['parse_error'] = e
				session_incorrect_objs.append(session)
	if(len(item_updates) > 0):
		print("UPDATING ITEMS")
		insert_gracefully(inteldb.item_collection, item_updates, ordered=True)
		item_updates = []
	if(len(activity_updates) > 0):
		print("UPDATED ACTIVITIES:")
		insert_gracefully(inteldb.activity_collection, activity_updates, ordered=True)
		activity_updates = []
	if(len(classroom_updates) > 0):
		insert_gracefully(inteldb.classroom_collection, classroom_updates, ordered=True)
		print("UPDATED CLASSROOMS:")
		classroom_updates = []
	if(len(student_updates) > 0):
		print("UPDATED STUDENTS:")
		insert_gracefully(inteldb.student_collection, student_updates, ordered=True)
		student_updates = []
	with open(fl_nm_reinsert_sessions, "w") as fp:
		json.dump({'Failed_Sessions':session_incorrect_objs}, fp, indent = 4, default=str)
	yield(session_updates, item_response_updates, len(session_incorrect_objs))
	# return(session_updates, item_response_updates)


def retrieve_learnosity_session_data_debug(inteldb, directory, df_activities, df_student_nodes, df_student_attempts, df_node_schema, last_sync_timestamp=default_date):
	'''
	# Given all the activities from before, (retrieve from Mongo) get any sessions. Return parsed sessions data
	# Also, update the students collection with newly inserted sessions. (Also filter out demo sessions.)
	'''
	fl_nm_reinsert_sessions = directory + "DB_Insert_Issues/Sessions/db_to_reinsert_"+ str(datetime.date.today()).replace('-','_') + ".json"
	activity_ref_list_full = list(df_activities['id'])
	session_start_sync_dt = datetime.datetime.now()
	#print("DEBUG", activity_ref_list_full)
	session_objs, session_updates = [], []
	item_updates, activity_updates, classroom_updates, student_updates = [], [], [], []
	item_response_objs, item_response_updates = [], []
	session_incorrect_objs = []
	for j in range(1, int(math.ceil(len(activity_ref_list_full)/read_batch_size))+1):
		activity_id_list=activity_ref_list_full[(read_batch_size*(j-1)):(read_batch_size*j)]
		data_request_session = {
			"activity_id": activity_id_list,
			"status": [
				"Completed"
				],
			"sort": "asc",
			"mintime": str(last_sync_timestamp),
			"maxtime": str(session_start_sync_dt),
		}
		client_learnosity = DataApi()
		res = client_learnosity.results_iter(cnfg.endpoint_sessions, cnfg.security, cnfg.consumer_secret, data_request_session, cnfg.action)
		av_cache = {}
		session_num = 0
		for idx, session in enumerate(res):
			if(len(item_updates) >= write_batch_size):
				print("UPDATING ITEMS")
				insert_gracefully(inteldb.item_collection, item_updates, ordered=True)
				item_updates = []
			if(len(activity_updates) >= write_batch_size):
				print("UPDATED ACTIVITIES:")
				insert_gracefully(inteldb.activity_collection, activity_updates, ordered=True)
				activity_updates = []
			if(len(classroom_updates) >= write_batch_size):
				print("UPDATED CLASSROOMS:")
				insert_gracefully(inteldb.classroom_collection, classroom_updates, ordered=True)
				classroom_updates = []
			if(len(student_updates) >= write_batch_size):
				print("UPDATED STUDENTS:")
				insert_gracefully(inteldb.student_collection, student_updates, ordered=True)
				student_updates = []
			# try:
			check = check_user_authenticity(inteldb, df_student_attempts, session['user_id'], session['activity_id'], session['session_id'])
			print("Session Number:", idx, len(item_updates), len(activity_updates), check, classroom_updates)
			if(check):
			# Above Statement Checks for dummy users and does not consider their responses
				try:
					# TODO: Get Other data: Assignment, Points Earned, Completed, pass criteria, valid_criteria
					student_node_row = df_student_nodes[(df_student_nodes['worksheet_id'] == session['activity_id']) & \
														(df_student_nodes['user_id'] == session['user_id'])]
					student_attempt_row = df_student_attempts[df_student_attempts['id'] == session['session_id']]
					#HOW TO GET NODE_ID
					node_id_set = set(list(student_node_row['node_id']))
					if(len(node_id_set) == 1):
						node_id = node_id_set.pop()
					elif(len(node_id_set) == 0):
						raise Exception('NO NODE for this session ', session['_id'], 'and activity ', session['activity_id'])
					else:
						#TODO: Address this particular case
						raise Exception("Student contains various nodes with same worksheet in them, Worksheet_id = ", session['activity_id'])
					node_linked = inteldb.node_collection.collection.find_one({'_id':node_id})
					if(node_linked is None):
						raise KeyError("NODE NOT FOUND", session['activity_id'], node_id)
					if(node_linked is not None and student_node_row.shape[0] == 1 and student_attempt_row.shape[0] != 0):
						session_parsed, item_responses_parsed, date_session_dict, item_updates, activity_updates = \
							parse_sessions(inteldb, session, node_linked, student_node_row.to_dict(orient='records')[0], student_attempt_row.to_dict(orient='records')[0], item_updates, activity_updates)
						session_updates.append(UpdateOne({'_id': session_parsed['_id']}, {'$set': session_parsed}, upsert = True))
						item_response_updates += [UpdateOne({'_id': item_response['_id']}, {'$set': item_response}, upsert = True) for item_response in item_responses_parsed]
						av_cache, classroom_updates, student_updates = populate_student_side(inteldb, node_linked, session_parsed, av_cache, classroom_updates, student_updates)
						if(len(session_updates) > cnfg.bulk_write_batch_size):
							print("Writing Data so far 1:", len(session_updates), len(item_response_updates))
							yield(session_updates, item_response_updates, -1)
							session_updates, item_response_updates = [], []
							print("Writing Data so far 2:", len(session_updates), len(item_response_updates))
				except IndexError:
					#print(session)
					print("INDEX_ERROR", session['user_id'], session['activity_id'], session['session_id'])
			# except Exception as e:
			# 	session['parse_error'] = e
			# 	session_incorrect_objs.append(session)
	if(len(item_updates) > 0):
		print("UPDATING ITEMS")
		insert_gracefully(inteldb.item_collection, item_updates, ordered=True)
		item_updates = []
	if(len(activity_updates) > 0):
		print("UPDATED ACTIVITIES:")
		insert_gracefully(inteldb.activity_collection, activity_updates, ordered=True)
		activity_updates = []
	if(len(classroom_updates) > 0):
		insert_gracefully(inteldb.classroom_collection, classroom_updates, ordered=True)
		print("UPDATED CLASSROOMS:")
		classroom_updates = []
	if(len(student_updates) > 0):
		print("UPDATED STUDENTS:")
		insert_gracefully(inteldb.student_collection, student_updates, ordered=True)
		student_updates = []
	with open(fl_nm_reinsert_sessions, "w") as fp:
		json.dump({'Failed_Sessions':session_incorrect_objs}, fp, indent = 4, default=str)
	yield(session_updates, item_response_updates, len(session_incorrect_objs))
	# return(session_updates, item_response_updates)


def save_schedule_data(df_schedules):
	sch_to_save = []
	for idx, row in df_schedules.iterrows():
		sch = inteldb.schedules_collection.collection.find_one({'_id':row['id']})
		if(sch is None):
			sch_to_save.append({'_id': row['id'], 'code':row['schedule_code']})
	if(len(sch_to_save) > 0):
		inteldb.schedules_collection.collection.insert_many(sch_to_save)


def update_student_node_data(df_student_nodes, df_activities, df_student_chapters, df_tags):
	# Given Assignment of any worksheet, update within student object.
	# print(df_student_worksheet)
	student_id_list = list(df_student_nodes['user_id'].unique())
	all_students = inteldb.student_collection.collection.find({'_id':{'$in':student_id_list}})
	all_stored_students = {}
	for std in all_students:
		all_stored_students[std.get('_id')] = std
	bulk_update_list = []
	for student_id in student_id_list:
		student = all_stored_students.get(student_id, None)
		if(student is None):
			print("Student does not exist. What to do?", student_id)
			continue
		student_chapter_dat = df_student_chapters[df_student_chapters.get('user_id') == student_id]
		student_progress = student.get('progress', {})
		if(student_chapter_dat.shape[0] == 0):
			continue
		for idx, row in student_chapter_dat.iterrows():
			if(not pd.isnull(row.get('created_on'))):
				dt_assigned = str(row.get('created_on'))
			else:
				dt_assigned = None
			if(not pd.isnull(row.get('started_on'))):
				started_on = str(row.get('started_on'))
			else:
				started_on = None
			if(not pd.isnull(row.get('completed_on'))):
				completed_on = str(row.get('completed_on'))
			else:
				completed_on = None
			if(student_progress.get(row.get('program_id'))):
				if(row.get('entity_type') == 'PROGRAM'):
					student_progress[row.get('program_id')].update({
						'dt_joined':dt_assigned,
						'started_on':started_on,
						'completed_on':completed_on,
						'program_code':row.get('program_code'),
						'program_name':row.get('program_name'),
						})
				else:
					student_progress[row.get('program_id')]['chapters_attempted'][row.get('entity_id')]={
						'dt_assigned':dt_assigned,
						'chapter_code':row.get('chapter_code'),
						'chapter_name':row.get('chapter_name'),
						'chapter_position':row.get('chapter_position'),
						'chapter_version':row.get('chapter_version'),
						'started_on':started_on,
						'completed_on':completed_on,
						# 'expected_time_complete':row.get('expected_time_complete'),
						# 'made_live_on':row.get('made_live_on')
						}
			else:
				if(row.get('entity_type') == 'PROGRAM'):
					student_progress[row.get('program_id')]={
						'dt_joined':dt_assigned,
						'started_on':started_on,
						'completed_on':completed_on,
						'program_code':row.get('program_code'),
						'program_name':row.get('program_name'),
						'chapters_attempted':{},
					}
				else:
					student_progress[row.get('program_id')] = {
						'chapters_attempted':{
							row.get('entity_id'):
								{
								'dt_assigned':dt_assigned,
								'chapter_code':row.get('chapter_code'),
								'chapter_name':row.get('chapter_name'),
								'chapter_position':row.get('chapter_position'),
								'chapter_version':row.get('chapter_version'),
								'started_on':started_on,
								'completed_on':completed_on,
								# 'expected_time_complete':row.get('expected_time_complete'),
								# 'made_live_on':row.get('made_live_on')
							}
						}
					}
		student_node_dat = df_student_nodes[df_student_nodes.get('user_id') == student_id]
		if(student_node_dat.shape[0] == 0):
			print("DEBUG, update_student_node_data no Node", student_id)
			continue
		student_lessons = student.get('lessons')
		student_homework = student.get('homework')
		student_assessments = student.get('assessments')
		student_remedials = student.get('remedials')
		for idx, row in student_node_dat.iterrows():
			worksheet_by_id = df_activities[df_activities['id']==row['worksheet_id']].iloc[0]
			worksheet_ref = worksheet_by_id['learnosity_activity_ref']
			worksheet_name = worksheet_by_id['name']
			complete_in = int(worksheet_by_id['complete_in'])
			if(row['course_type'] == 'PROGRAM' and row['is_timed'] == 'false' and row['attempt_location']=='INCLASS'):
				if(student_lessons.get(row['worksheet_id'], None) is None \
					or student_lessons.get(row['worksheet_id']).get('complete_status')!= get_boolean_from_string(row['complete_status'])\
					or student_lessons.get(row['worksheet_id']).get('reattempt_status')!= get_boolean_from_string(row['reattempt_status'])\
					or student_lessons.get(row['worksheet_id']).get('unlock_status')!= get_boolean_from_string(row['unlock_status'])):
						student_lessons[row['worksheet_id']] = {
																	'dt_assigned':str(row['created_on']),
																 'dt_due': str(row['created_on'] + datetime.timedelta(days=complete_in)),
																 'activity_reference':worksheet_ref,
																 'activity_name':worksheet_name,
																 'sessions_attempted':student_lessons.get(row['worksheet_id'], {}).get('sessions_attempted',{}),
																 'node_id':row['node_id'],
																 'node_type':'LESSON',
																 'program_id':row['program_id'],
																 'complete_status':get_boolean_from_string(row['complete_status']),
																 'reattempt_status':get_boolean_from_string(row['reattempt_status']),
																 'unlock_status':get_boolean_from_string(row['unlock_status']),
																 'chapter_id':row['chapter_id'],
																 }
			elif(row['course_type'] == 'PROGRAM' and row['is_timed'] == 'false' and row['attempt_location']!= 'INCLASS'):
				if(student_homework.get(row['worksheet_id'], None) is None  \
					or student_homework.get(row['worksheet_id']).get('complete_status')!= get_boolean_from_string(row['complete_status'])\
					or student_homework.get(row['worksheet_id']).get('reattempt_status')!= get_boolean_from_string(row['reattempt_status'])\
					or student_homework.get(row['worksheet_id']).get('unlock_status')!= get_boolean_from_string(row['unlock_status'])):
					student_homework[row['worksheet_id']] = {'dt_assigned':str(row['created_on']),
																 'dt_due': str(row['created_on'] + datetime.timedelta(days=complete_in)),
																 'activity_reference':worksheet_ref,
																 'activity_name':worksheet_name,
																 'sessions_attempted':student_homework.get(row['worksheet_id'], {}).get('sessions_attempted',{}),
																 'node_id':row['node_id'],
																 'node_type':'HOMEWORK',
																 'program_id':row['program_id'],
																 'complete_status':get_boolean_from_string(row['complete_status']),
																 'reattempt_status':get_boolean_from_string(row['reattempt_status']),
																 'unlock_status':get_boolean_from_string(row['unlock_status']),
																 'chapter_id':row['chapter_id'],
																 }
			elif(row['course_type'] == 'PROGRAM' and row['is_timed'] == 'true'):
				if(student_assessments.get(row['worksheet_id'], None) is None  \
					or student_assessments.get(row['worksheet_id']).get('complete_status')!= get_boolean_from_string(row['complete_status'])\
					or student_assessments.get(row['worksheet_id']).get('reattempt_status')!= get_boolean_from_string(row['reattempt_status'])\
					or student_assessments.get(row['worksheet_id']).get('unlock_status')!= get_boolean_from_string(row['unlock_status'])):
					tag_data = df_tags[(df_tags['ent.entity_type']=='NODE') & (df_tags['ent.entity_id']==row['node_id'])]
					if(tag_data.shape[0] == 0):
						sheet_type = 'CHAPTER_ASSESSMENT'
					elif(tag_data.shape[0] == 1):
						sheet_type = 'BLOCK_ASSESSMENT'
					student_assessments[row['worksheet_id']] = {'dt_assigned':str(row['created_on']),
																'dt_due': str(row['created_on'] + datetime.timedelta(days=complete_in)),
															 	'activity_reference':worksheet_ref,
															 	'activity_name':worksheet_name,
															 	'sessions_attempted':student_assessments.get(row['worksheet_id'], {}).get('sessions_attempted',{}),
															 	'node_id':row['node_id'],
															 	'node_type':sheet_type,
															 	'program_id':row['program_id'],
															 	'complete_status':get_boolean_from_string(row['complete_status']),
																'reattempt_status':get_boolean_from_string(row['reattempt_status']),
															 	'unlock_status':get_boolean_from_string(row['unlock_status']),
															 	'chapter_id':row['chapter_id'],
																}
			elif(row['course_type'] == 'REMEDIAL'):
				if(student_remedials.get(row['worksheet_id'], None) is None  \
					or student_remedials.get(row['worksheet_id']).get('complete_status')!= get_boolean_from_string(row['complete_status'])\
					or student_remedials.get(row['worksheet_id']).get('reattempt_status')!= get_boolean_from_string(row['reattempt_status'])\
					or student_remedials.get(row['worksheet_id']).get('unlock_status')!= get_boolean_from_string(row['unlock_status'])):
					student_remedials[row['worksheet_id']] = {'dt_assigned':str(row['created_on']),
																'dt_due': str(row['created_on'] + datetime.timedelta(days=complete_in)),
																'activity_reference':worksheet_ref,
																'activity_name':worksheet_name,
																'sessions_attempted':student_remedials.get(row['worksheet_id'], {}).get('sessions_attempted',{}),
																'node_id':row['node_id'],
																'node_type':'REMEDIAL',
																'program_id':row['program_id'],
																'complete_status':get_boolean_from_string(row['complete_status']),
																'reattempt_status':get_boolean_from_string(row['reattempt_status']),
																'unlock_status':get_boolean_from_string(row['unlock_status']),
																'chapter_id':row['chapter_id'],
																}
		newvalues = {'$set':{
								'progress':student_progress,
								'lessons': student_lessons,
								'homework':student_homework,
								'assessments':student_assessments,
								'remedials':student_remedials
							}
					}
		bulk_update_list.append(UpdateOne({'_id':student_id}, newvalues))
	if(len(bulk_update_list)>0):
		try:
			insert_gracefully(inteldb.student_collection, bulk_update_list, ordered = True)
		except pymongo.errors.BulkWriteError as bwe:
			print(bwe.details)

def get_primary_program(student_program_dat):
	archived_programs = []
	i = 0
	max_started_on_row = None
	for idx, row in student_program_dat.iterrows():
		if(row.get('program_code').startswith('DEMO')):
			continue
			# print("DEBUG get_primary_program", idx, row)
		if(max_started_on_row is None and i>=0):
			max_started_on_row = row
		elif(row.get('started_on') > max_started_on_row.get('started_on')):
			archived_programs.append(max_started_on_row.get('program_code'))
			max_started_on_row = row
		else:
			archived_programs.append({row.get('program_id'):row.get('program_code')})
		i = i + 1
	if(max_started_on_row is None):
		return(None, None, None)
	return(max_started_on_row.get('program_code'), max_started_on_row.get('grade'), archived_programs)


def update_students(df_students, student_list, df_student_program, df_student_worksheet):
	'''
	Save all students information; Edit info when reqd.
	Shift df calls to update_db_script
	'''
	df_student_slots = IntelDB.get_student_slots()
	student_objs_to_save = []
	students_to_remove = []
	all_students_stored = inteldb.student_collection.collection.find({'_id':{'$in':student_list}})
	all_students = {}
	for std in all_students_stored:
		all_students[std.get('_id')] = std
	for student_id in student_list:
		toggle_US = False
		student_stored = all_students.get(student_id, None)
		toggle_stored = False
		toggle_slot_stored = False
		toggle_program_changed = False
		student_data = df_students[df_students['id']==student_id]
		student_slot_data = df_student_slots[df_student_slots['student_slots_student_id']==student_id].sort_values(by='student_slots_updated_on', ascending=False)
		student_program_dat = df_student_program[(df_student_program['user_id']==student_id)]
		current_program, current_grade, archived_programs = get_primary_program(student_program_dat)
		if(student_data.shape[0] == 0  or current_program is None):
			if(toggle_US):
				print('US.TEST 1.25', student_data.shape[0] == 0, student_slot_data, current_program)
			continue
		elif(student_data.shape[0] > 1):
			raise Exception("Multiple Instances of Student Found")
			#raise Exception("Multiple Instances of Student Slot Found")
		if(student_slot_data.shape[0]== 0):
			primary_student_slot_data = None
		else:
			primary_student_slot_data = IntelDB.get_primary_slots_and_teacher(student_slot_data)
			if(primary_student_slot_data is None):
				primary_student_slot_data = df_student_slots.iloc[0]
		try:
			if(student_stored):
				toggle_stored = (student_data.iloc[0]['updated_on'].to_pydatetime().replace(microsecond=0, second=0) > student_stored['updated_on'].replace(microsecond=0, second=0))
				if(primary_student_slot_data is not None and student_stored.get('slot_created_on') is not None):
					toggle_slot_stored = (primary_student_slot_data['student_slots_created_on'].to_pydatetime().replace(microsecond=0, second=0)>student_stored['slot_created_on'].replace(microsecond=0, second=0))
				# print('LULZ', (primary_student_slot_data['student_slots_updated_on'].to_pydatetime().replace(microsecond=0, second=0, minute=0), student_stored['slot_updated_on'].replace(microsecond=0, second=0, minute=0)))
				toggle_program_changed = (student_stored['current_program'] != current_program)
		except TypeError as tp:
			print('TypeError on Student', type(student_stored['updated_on']), 
											type(student_stored['slot_updated_on']),
											type(student_data.iloc[0]['updated_on']),
											type(primary_student_slot_data['student_slots_updated_on']),
											tp
											)
			pass
		except KeyError:
			print('KeyError on Student')
			pass
		if(toggle_US):
			print('US.TEST 2')
		if(student_stored is None or toggle_stored or toggle_slot_stored or toggle_program_changed):
			print("Student being updated", toggle_stored, toggle_slot_stored)
			student_dat = student_data.to_dict(orient='records')[0]
			if(student_id == 'd8f77d00-4bc1-11e9-bb11-02420a00fae3'):
				student_dat['email'] ='vinaymenon1@gmail.com'
			if(student_id=='61fb5abc-2f7b-11e9-9943-02420a00f11a'):
				student_dat['email'] ='not.provided.by.mentorship.team@gmail.com'
			if(student_id=='f95e5c02-89da-11e9-b0f0-02420a00f13a'):
				student_dat['email'] ='charugera1976@yahoo.co.in'
			try:
				#print(student_dat['email'])
				if(not pd.isnull(student_dat['email']) and student_dat['email'].find(".training@cuemath.com") != -1):
					#print("TEACHER")
					is_demo = False
					is_teacher = True
				elif(not pd.isnull(student_dat['email']) and student_dat['email'].find("@cuemath.com") == -1 and student_dat['email']!=''):
					is_demo = False
					is_teacher = False
				else:
					is_demo = True
					is_teacher = False
				if(student_stored is not None):
					is_demo = student_stored.get('is_demo')
					is_teacher = student_stored.get('is_teacher')
			except AttributeError:
				print("Something Wrong with object")
				import pdb; pdb.set_trace()
					#demo_std +=1
				# Create New Student Object if not already present. Add is_demo field to it.
			if(not pd.isnull(student_dat['email'])):
				email = student_dat['email'].strip()
			else:
				email = ''
			student_obj = {'_id': student_dat['id'], \
				'created_on':student_dat['created_on'], \
				'updated_on':student_dat['updated_on'], \
				'email':email, \
				'username':student_dat['username'],
				'phone':student_dat['phone'], \
				'first_name':student_dat['first_name'], \
				'last_name':student_dat['last_name'],\
				'gender':student_dat['gender'], \
				'is_demo':is_demo, \
				'is_teacher':is_teacher, \
				'current_program':current_program,\
				'current_grade':current_grade,\
				}
			# Get student_stored's slot data and hence teacher data: Cycle and check for updates.
			# TODO: Assign Primary and Additional Teachers, Slots.
			if(primary_student_slot_data is not None):
				student_obj['teacher_id'] = primary_student_slot_data['teacher_slots_teacher_id']
				student_obj['is_demo_slot'] = bool(primary_student_slot_data['student_slots_is_demo'])
				student_obj['slot_duration'] = int(primary_student_slot_data['teacher_slots_duration'])
				student_obj['slot_created_on'] = primary_student_slot_data['student_slots_created_on']
				student_obj['slot_updated_on'] = primary_student_slot_data['student_slots_updated_on']
				student_obj['schedule_id'] = primary_student_slot_data['teacher_slots_schedule_id']
			else:
				student_obj['teacher_id'] = '12345'
				student_obj['is_demo_slot'] = None
				student_obj['slot_duration'] = None
				student_obj['slot_created_on'] = None
				student_obj['slot_updated_on'] = None
				student_obj['schedule_id'] = None
			if(student_slot_data.shape[0] >= 1):
				for idx, row in student_slot_data.iterrows():
					if(row['student_slots_id'] == primary_student_slot_data['student_slots_id']):
						continue
					try:
						student_obj['additional_slots'].append({'schedule_id':row['teacher_slots_schedule_id'],\
																'teacher_id':row['teacher_slots_teacher_id'],\
																'status':row['student_slots__status'],\
																'type':row['student_slots__type'],\
																})
					except KeyError:
						student_obj['additional_slots'] = [{'schedule_id':row['teacher_slots_schedule_id'],\
																'teacher_id':row['teacher_slots_teacher_id'],\
																'status':row['student_slots__status'],\
																'type':row['student_slots__type'],\
																}]
			else:
				student_obj['additional_slots'] = []
		else:
			continue
		if(student_stored is None):
			student_obj.update({'lessons':{}, 'homework':{},'remedials':{},'assessments':{}, 'progress':{}})
		student_objs_to_save.append(UpdateOne({'_id':student_obj['_id']}, {'$set':student_obj}, upsert = True))
	return(student_objs_to_save)

def update_classroom_raw(df_student_classroom, df_student_classroom_ratings):
	'''
	Get Classroom Objects from the data frame
	'''
	# print("last_sync_dt", last_sync_dt)
	classroom_objs_to_save = []
	rep = 1
	# print("LEN_CLASSROOM", df_student_classroom.shape[0])
	classroom_id_list = list(df_student_classroom['student_classroom_id'])
	all_existing_classrooms_cursor = inteldb.classroom_collection.collection.find({'_id':{'$in':classroom_id_list}})
	all_existing_classrooms = {}
	for classrm in all_existing_classrooms_cursor:
		all_existing_classrooms[classrm.get('_id')] = classrm
	for idx, row in df_student_classroom.iterrows():
		existing_classroom = all_existing_classrooms.get(row['student_classroom_id'])
		if(existing_classroom is not None):
			if(existing_classroom['status']==row['student_classroom__status'] and existing_classroom['exit_time']==str(row['student_classroom_exit_time'])):
				continue
		class_end_tm = datetime.datetime.strptime(row['student_classroom_scheduled_end_time'], '%H:%M:%S').time()
		class_end_dt_time = datetime.datetime.combine(row['student_classroom_date'], class_end_tm)
		student_rating_row = df_student_classroom_ratings[df_student_classroom_ratings['student_classroom_id']==row['student_classroom_id']]
		if(student_rating_row.shape[0]!=0):
			rating_list = student_rating_row.to_dict(orient='records')
			rating_obj = rating_list[0]
			rating_obj['rating'] = float(rating_obj['rating'])
			rating_obj['created_on'] = str(rating_obj['created_on'])
			rating_obj['id'] = str(rating_obj['id'])
			del rating_obj['student_classroom_id']
			if(len(rating_list) > 1):
				raise Exception("len(rating_list) > 1 inside update_classroom_raw")
		else:
			rating_obj = {}
		if(existing_classroom is None):
			classroom_obj = {"_id":row['student_classroom_id'],\
						"teacher_classroom_id":row['teacher_classroom_id'],\
						"student_id":row['student_classroom_student_id'],\
						"teacher_id":row['teacher_classroom_teacher_id'],\
						"date":str(row['student_classroom_date']),\
						"status":row['student_classroom__status'],\
						"teacher_num_attendees":row['teacher_classroom_attendance'],\
						"teacher_status":row['teacher_classroom__status'],\
						"scheduled_start_time":str(row["student_classroom_scheduled_start_time"]),\
						"scheduled_end_time": str(row["student_classroom_scheduled_end_time"]),\
						"sessions_attempted":{},
						"av_data":{},
						"ratings":rating_obj,
						}
		else:
			classroom_obj = {"_id":row['student_classroom_id'],\
						"student_id":row['student_classroom_student_id'],\
						"teacher_classroom_id":row['teacher_classroom_id'],\
						"teacher_id":row['teacher_classroom_teacher_id'],\
						"date":str(row['student_classroom_date']),\
						"status":row['student_classroom__status'],\
						"teacher_num_attendees":row['teacher_classroom_attendance'],\
						"teacher_status":row['teacher_classroom__status'],\
						"scheduled_start_time":str(row["student_classroom_scheduled_start_time"]),\
						"scheduled_end_time": str(row["student_classroom_scheduled_end_time"]),\
						"sessions_attempted":existing_classroom.get('sessions_attempted',{}),
						"av_data":existing_classroom.get('av_data',{}),
						"ratings":existing_classroom.get('ratings', rating_obj),
						}
		try:
			if(row['student_classroom__status'] == "P"):
				try:
					classroom_obj['enter_time'] = str(row["student_classroom_enter_time"])
					classroom_obj['exit_time'] = str(row["student_classroom_exit_time"])
					classroom_obj['teacher_enter_time'] = str(row['teacher_classroom_enter_time'])
					classroom_obj['teacher_exit_time'] = str(row['teacher_classroom_exit_time'])
				except TypeError:
					print("TypeError: ", row['student_classroom_id'], row['student_classroom_date'], row["student_classroom_enter_time"], row["student_classroom_exit_time"])
					continue
			elif(row['student_classroom__status'] == "A"):
				try:
					classroom_obj['enter_time'] = None
					classroom_obj['exit_time'] = None
					classroom_obj['teacher_enter_time'] = str(row['teacher_classroom_enter_time'])
					classroom_obj['teacher_exit_time'] = str(row['teacher_classroom_exit_time'])
				except TypeError:
					print("TypeError: ", row['student_classroom_id'], row["teacher_classroom_enter_time"], row["teacher_classroom_exit_time"])
					#continue
			elif(row['student_classroom__status'] == "TA"):
				classroom_obj['enter_time'] = None
				classroom_obj['exit_time'] = None
				classroom_obj['teacher_enter_time'] = None
				classroom_obj['teacher_exit_time'] = None
		except KeyError:
			continue
		classroom_objs_to_save.append(UpdateOne({'_id':classroom_obj['_id']}, {'$set':classroom_obj}, upsert = True))
	return(classroom_objs_to_save)

def update_teachers():
	'''
	Update Teacher and their Slot data if required.
	'''
	df_teachers = IntelDB.get_all_teachers()
	df_teacher_slots = IntelDB.get_teacher_slots()
	teacher_id_list = list(df_teachers['id'])
	teacher_objs_to_save = []
	teachers_to_remove = []
	all_teachers_cursor = inteldb.teacher_collection.collection.find({'_id':{'$in':teacher_id_list}})
	all_teachers = {}
	for tch in all_teachers_cursor:
		all_teachers[tch.get('_id')] = tch
	for teacher_id in teacher_id_list:
		toggle_stored = False
		toggle_slot_stored = False
		teacher_stored = all_teachers.get(teacher_id)
		teacher_data = df_teachers[df_teachers['id']==teacher_id]
		teacher_slot_data = df_teacher_slots[df_teacher_slots['teacher_id']==teacher_id].sort_values(by='updated_on', ascending=False)
		if(teacher_data.shape[0] == 0 or teacher_slot_data.shape[0] == 0):
			continue
		elif(teacher_data.shape[0] != 1):
			raise Exception("Multiple Instances of Teacher Found")
		try:
			toggle_stored = (teacher_data.iloc[0]['updated_on'].replace(microsecond=0, second=0) > teacher_stored['updated_on'].replace(microsecond=0, second=0))
			toggle_slot_stored = (teacher_slot_data.iloc[0]['updated_on'].replace(microsecond=0, second=0)>teacher_stored['slot_updated_on'].replace(microsecond=0, second=0))
			# toggle_slot_stored= True
		except TypeError:
			pass
		if(teacher_stored is None or toggle_stored or toggle_slot_stored):
			teacher_obj = {'_id': teacher_data.iloc[0]['id'],
					'created_on':teacher_data.iloc[0]['created_on'],
					'updated_on':teacher_data.iloc[0]['updated_on'],
					'email':teacher_data.iloc[0]['email'],
					'phone':teacher_data.iloc[0]['phone'],
					'first_name':teacher_data.iloc[0]['first_name'],
					'last_name':teacher_data.iloc[0]['last_name'],
					'gender':teacher_data.iloc[0]['gender'],
					'is_demo':get_boolean_from_string(teacher_data.iloc[0]['is_demo']),
					}
			teacher_obj['is_demo_slot'] = bool(teacher_slot_data.iloc[0]['is_demo'])
			teacher_obj['grades'] = teacher_slot_data.iloc[0]['grades']
			teacher_obj['duration'] = int(teacher_slot_data.iloc[0]['duration'])
			teacher_obj['slot_created_on'] = teacher_slot_data.iloc[0]['created_on']
			teacher_obj['slot_updated_on'] = teacher_slot_data.iloc[0]['updated_on']
			teacher_obj['additional_slots'] = []
			for idx, row in teacher_slot_data.iterrows():
				# print('HEYYYYYY')
				teacher_obj['additional_slots'].append({'schedule_id':row['schedule_id'],\
																								'status':row['_status'],\
																								'type':row['_type']})
			try:
				del teacher_obj['additional_schedule_ids']
				del teacher_obj['schedule_id']
			except KeyError:
				pass
			#if(toggle_stored or toggle_slot_stored):
			#	teachers_to_remove.append(teacher_id)
			teacher_objs_to_save.append(UpdateOne({'_id':teacher_obj['_id']}, {'$set':teacher_obj}, upsert = True))
		# Does not add object if no changes made.
	#if(len(teachers_to_remove)!=0):
	#	inteldb.teacher_collection.collection.remove({'_id':{'$in':teachers_to_remove}})
	return(teacher_objs_to_save)

def parse_av_data(av_cache_dt, user_id, teacher_id, date_dict, session_id, dt, class_strt_tm):
	'''
	Parses AV Data into call and doubt data.
	'''
	# print("INSIDE PARSE_AV_DATA")
	df_av_monitor, df_av_request, df_av_whiteboard = av_cache_dt
	strt_tm = 0
	toggle_calculate = False
	teacher_stream_toggle = False
	calls = []
	all_sessions = []
	dropped_calls = 0
	# print(av_cache_dt)
	# print(df_av_monitor.sort_values('created_on'))
	#try:
	if(class_strt_tm):
		class_enter_dt_time = datetime.datetime.combine(datetime.datetime.strptime(dt, "%Y-%m-%d"), datetime.datetime.strptime(class_strt_tm, "%H:%M:%S").time())
	else:
		# print("class_strt_tm 1", class_strt_tm)
		class_enter_dt_time = datetime.datetime.strptime(dt, "%Y-%m-%d")
	connect_attempts = 0
	teacher_stream_strt_tm = None
	for idx, av_row in df_av_monitor.sort_values('created_on').iterrows():
		if((av_row['event']=='streamCreated' and av_row['user_role'] == 'moderator')):
			## TODO: Ensure every row belongs to 
			# print("Teacher Stream Created")
			connect_attempts += 1
			if(not teacher_stream_toggle and av_row['created_on']> class_enter_dt_time):
				teacher_stream_strt_tm = av_row['created_on'] - datetime.timedelta(hours =5, minutes= 30)
				teacher_stream_toggle = True
		if(av_row['event']=='streamCreated' and av_row['user_role'] == 'publisher'):
			# print("Student Stream Created")
			strt_tm = av_row['created_on'] - datetime.timedelta(hours =5, minutes= 30)
		elif(strt_tm!=0 and av_row['event']=='streamDestroyed'):
			# print('Stream Destroyed')
			end_tm = av_row['created_on'] - datetime.timedelta(hours =5, minutes= 30)
			toggle_calculate = True
			teacher_stream_toggle = False
		#print("HI")
		if(toggle_calculate):
			call_duration = (end_tm-strt_tm).total_seconds()
			if(teacher_stream_strt_tm is not None and not teacher_stream_toggle):
				call_lag = (strt_tm - teacher_stream_strt_tm).total_seconds()
			else:
				print("CALL_LAG_DATA_UNAVAILABLE", user_id, teacher_id, session_id, dt)
				call_lag = None
				# connect_attempts = 1
			#if(call_duration<0):
			#	import pdb; pdb.set_trace()
			this_call_obj = {'start_time':strt_tm, 'end_time':end_tm, 'duration_in_seconds':call_duration, 'call_stream_lag':call_lag, 'connect_attempts':connect_attempts, 'teacher_stream_start_time':teacher_stream_strt_tm}
			calls.append(this_call_obj)
			# print("DEBUG_CALL_OBJ ", this_call_obj)
			connect_attempts = 0
			if((end_tm-strt_tm).total_seconds()>cnfg.min_call_time_for_video_logs):
				all_sessions.append(av_row["session_id"])
			else:
				dropped_calls += 1
			toggle_calculate = False
			strt_tm = 0
	#print(calls)
	# print("AFTER CALLS PARSE_AV_DATA")
	# calls['dropped_calls'] = dropped_calls
	toggle_tm_1 = True
	tm2 = None
	for idx, call_obj in enumerate(calls):
		if(idx>0):
			tm2 = call_obj['start_time']
			#toggle_tm_1 = False
			tm1 = calls[(idx-1)]['start_time']
			#toggle_tm_1 = True
		#if(tm2 is not None):
			#print(idx, tm1, tm2)#, df_av_whiteboard[(df_av_whiteboard['created_on'] > tm1) & (df_av_whiteboard['created_on'] < tm2)])
			if(tm1<tm2):
				df_this_wtbrd = df_av_whiteboard[(df_av_whiteboard['created_on'] > tm1) & (df_av_whiteboard['created_on'] < tm2)]
				for ix, row in df_this_wtbrd.iterrows():
					try:
						call_obj['whiteboards'].update({row['id']:{'created_on':row['created_on'], 'image_url':row['image_url']}})
					except KeyError:
						call_obj['whiteboards'] = {row['id']:{'created_on':row['created_on'], 'image_url':row['image_url']}}
					except JSONDecodeError:
						continue
	dt_list = []
	key_list = []
	# print("AFTER WHITEBOARD PARSE_AV_DATA")
	for k_item_response, v_item_response in date_dict['item_response_dict'].items():
		for k_ques, v_ques in v_item_response.items():
			try:
				v_ques = datetime.datetime.strptime(v_ques, "%Y-%m-%dT%H:%M:%SZ")
			except ValueError:
				v_ques = datetime.datetime.strptime(v_ques, "%Y-%m-%d %H:%M:%S")
			# IMPORTANT: The times appear to be at GMT time according to the request button.
			v_ques_local = v_ques #+ datetime.timedelta(hours =5, minutes= 30)
			dt_list.append(v_ques_local)
			key_list.append({'session_id': session_id, 'question_id':k_ques, 'item_response_id':k_item_response, "created_on":str(v_ques_local)})
	# Sort dt_list index
	sorted_indices = argsort(dt_list)
	# Given sorted dates, identify where doubt was raised.
	prev_i = sorted_indices[0]
	doubt_objs = {}
	for idx, i in enumerate(sorted_indices[1:(len(sorted_indices)-1)]):
		tm2 = dt_list[i]
		tm1 = dt_list[prev_i]
		if(idx<(len(sorted_indices)-1)):
			df_av_doubts = df_av_request[(df_av_request['created_on'] >= tm1) & (df_av_request['created_on'] <= tm2)]
		else:
			df_av_doubts = df_av_request[(df_av_request['created_on'] >= tm2)]
		for ix, row in df_av_doubts.iterrows():
			if(row.get('meta')):
				try:
					meta_dict = json.loads(row['meta'])
					num_dbts = len(meta_dict.get('doubts', [0]))
				except json.decoder.JSONDecodeError:
					num_dbts = 0
			else:
				num_dbts = 1
			try:
				doubt_objs[row['id']].update({"raised_at":key_list[prev_i], "meta":json.loads(row["meta"]), 'num_button_press':num_dbts})
			except KeyError:
				try:
					doubt_objs[row['id']] = {"raised_at":key_list[prev_i], "meta":json.loads(row["meta"]), 'num_button_press':num_dbts}
				except json.decoder.JSONDecodeError:
					continue
		prev_i = i
	for ix, row in df_av_request.iterrows():
		try:
			if(doubt_objs.get(row['id'], None) is None):
				try:
					# print("DEBUG", ast.literal_eval(row["meta"]), type(ast.literal_eval(row['meta'])))
					try:
						meta_dict = json.loads(row['meta'])
					except ValueError:
						# import pdb; pdb.set_trace()
						raise KeyError
					num_dbts = len(meta_dict.get('doubts', [0]))
				except KeyError:
					num_dbts = 1
				try:
					doubt_objs[row['id']].update({"raised_at":{'session_id': session_id, 'question_id':None, 'item_response_id':None, "created_on":row['created_on']}, "meta":json.loads(row["meta"]), 'num_button_press':num_dbts})
				except KeyError:
					doubt_objs[row['id']] = {"raised_at":{'session_id': session_id, 'question_id':None, 'item_response_id':None, "created_on":row['created_on']}, "meta":json.loads(row["meta"]), 'num_button_press':num_dbts}
		except json.decoder.JSONDecodeError:
			continue
	av_session_list = list(set(all_sessions))
	# print("AFTER DOUBTS PARSE_AV_DATA")
	if(len(av_session_list) > 0):
		video_logs = IntelDB.get_video_log_data(list(set(all_sessions)))
	else:
		video_logs = []
	av_data = {'call_data':calls, 'doubts_raised':doubt_objs, "video_logs":video_logs, 'dropped_calls':dropped_calls}
	return(av_data)


def insert_gracefully(obj_collection, updates, **kwargs):
	for i in range(60):
		try:
			result = obj_collection.collection.bulk_write(updates, **kwargs)
			print('wrote ', len(updates), 'objects', result.modified_count, result.inserted_count, result.upserted_count, result.matched_count)
			break # Exit the retry loop
		except pymongo.errors.AutoReconnect as e:
			print('Warning', e)
			time.sleep(5)
	else:
		raise Exception("Couldn't write!")

def update_all_dbs(directory, last_sync_obj, instance_str):
	# Define a status object where read and write operations are monitored;
	if(not os.path.isdir(directory+"DB_Insert_Issues/")):
		os.makedirs(directory+"DB_Insert_Issues/Sessions/")
		os.makedirs(directory+"DB_Insert_Issues/Activities")
		os.makedirs(directory+"DB_Insert_Issues/Items/")
	if(not os.path.isdir(directory+"Frontend/tmp/")):
		os.makedirs(directory+"Frontend/tmp/")
	status = dict()
	status['dt_sync_started'] = datetime.datetime.now()
	status['items_updated'] = False
	status['items_inserted'] = 0
	status['items_errors'] = []
	status['students_updated'] = False
	status['students_inserted'] = 0
	status['students_errors'] = []
	status['teachers_updated'] = False
	status['teachers_inserted'] = 0
	status['teachers_errors'] = []
	status['classrooms_updated'] = False
	status['classrooms_inserted'] = 0
	status['classrooms_errors'] = []
	status['activities_updated'] = False
	status['activities_inserted'] = 0
	status['activities_errors'] = []
	status['sessions_updated'] = False
	status['sessions_inserted'] = 0
	status['sessions_errors'] = []
	status['item_responses_updated'] = False
	status['item_responses_inserted'] = 0
	status['item_responses_errors'] = []
	status['sync_completed'] = False
	#try:
	print("CUEMATH DB Access Started")
	# Connect to local db to find if any Worksheets newly created since last_sync_timestamp: Define in while loop so program only moves once connection established with local db.
	global inteldb
	inteldb = IntelDB(instance_str)
	df_activities = IntelDB.get_worksheet_db_data()
	# print(df_activities[df_activities['id']== '568ed03a-131f-11e9-a40a-4a000536b220'])
	df_node_schema = IntelDB.get_node_schema()
	df_tags = IntelDB.get_tags_data()
	df_chapters = IntelDB.get_all_chapter_list();
	chapter_list_mngo = inteldb.chapter_collection.collection.find()
	chapter_id_list_mngo = [chapter.get('_id') for chapter in chapter_list_mngo]
	chp_to_insert = []
	for idx, chp in df_chapters.iterrows():
	    if(chp.get('chapter_code') not in chapter_id_list_mngo):
	        chp_to_insert.append(UpdateOne({'_id':chp['chapter_code']},
	                            {'$set':{
	                            'program_code':chp['program_code'],
	                            'name':chp['chapter_name'],
	                            'is_live':False
	                            }}, upsert=True))
	if(len(chp_to_insert) != 0):
	    inteldb.chapter_collection.collection.bulk_write(chp_to_insert)
	df_programs = IntelDB.get_all_program_list();
	program_list_mngo = inteldb.program_collection.collection.find()
	program_id_list_mngo = [program.get('_id') for program in program_list_mngo]
	prg_to_insert = []
	for idx, prg in df_programs.iterrows():
	    if(prg.get('_id') not in program_id_list_mngo):
	        prg_to_insert.append(UpdateOne({'_id':prg['code']},
	                            {'$set':{
	                            'name':prg['name'],
	                            'is_live':False,
	                            'grade':prg['grade'],
	                            'prg_id':prg['id'],
	                            }}, upsert=True))
	if(len(prg_to_insert) != 0):
	    inteldb.program_collection.collection.bulk_write(prg_to_insert)
	df_student_programs = IntelDB.get_student_program_data()
	df_student_chapters = IntelDB.get_student_chapter_data(last_sync_obj['sessions'])
	student_list = list(set(df_student_programs['user_id']))
	df_students = IntelDB.get_student_data(student_list)
	df_schedules = IntelDB.get_schedules()
	df_student_classroom = IntelDB.get_classroom_data(last_sync_obj['classrooms'])
	df_student_classroom_ratings = IntelDB.get_classroom_ratings_data(last_sync_obj['classrooms'])
	df_student_attempts = IntelDB.get_student_attempts_data(last_sync_obj['sessions'])
	df_student_nodes = IntelDB.get_student_node_data(last_sync_obj['sessions'])
	save_schedule_data(df_schedules)
	with open(directory +"Frontend/tmp/users_not_on_cuedb.csv", "w") as fp:
		fp.write("User_ID \n")
	print("CUEMATH DB Access Ended")
	#except Define Connection Error here and time out and wait for db to up and running again.
	if(last_sync_obj['update_students']):
		new_students = update_students(df_students, student_list, df_student_programs, df_student_nodes)
		if(len(new_students)>0):
			try:
				insert_gracefully(inteldb.student_collection, new_students, ordered=True)
				print("STUDENTS WORKED", len(new_students))
				status['students_updated'] = True
				status['students_inserted'] = len(new_students)
			except pymongo.errors.BulkWriteError as bwe:
				status['students_updated'] = False
				status['students_inserted'] = 0
				status['students_errors'].append(bwe.details)
		else:
			status['students_updated'] = True
			status['students_inserted'] = len(new_students)
			status['students_errors'].append("No New Students Today")
	if(last_sync_obj['update_teachers']):
		new_teachers = update_teachers()
		try:
			if(len(new_teachers)>0):
				try:
					insert_gracefully(inteldb.teacher_collection, new_teachers, ordered=True)
					print("TEACHERS WORKED", len(new_teachers))
					status['teachers_updated'] = True
					status['teachers_inserted'] = len(new_teachers)
				except pymongo.errors.BulkWriteError as bwe:
					status['teachers_updated'] = False
					status['teachers_inserted'] = 0
					status['teachers_errors'].append(bwe.details)
			else:
				status['teachers_updated'] = True
				status['teachers_inserted'] = len(new_teachers)
				status['teachers_errors'].append("No New Teachers Today")
		except UnboundLocalError:
			pass
	if(last_sync_obj['update_classrooms']):
		new_classroom_updates = update_classroom_raw(df_student_classroom, df_student_classroom_ratings)
		try:
			if(len(new_classroom_updates)>0):
				try:
					insert_gracefully(inteldb.classroom_collection, new_classroom_updates, ordered=True)
					print("CLASSROOMS WORKED", len(new_classroom_updates))
					status['classrooms_updated'] = True
					status['classrooms_inserted'] += len(new_classroom_updates)
				except pymongo.errors.BulkWriteError as bwe:
					status['classrooms_updated'] = False
					#status['students_inserted'] = 0
					status['classrooms_errors'].append(bwe.details)
			else:
				status['classrooms_updated'] = True
				status['classrooms_inserted'] = len(new_classroom_updates)
				status['classrooms_errors'].append("No New Students Today")
		except UnboundLocalError:
			pass
	if(last_sync_obj['update_items']):
		try:
			new_item_updates = update_items(directory, df_activities, last_sync_obj['items'])
		except learnosity_sdk.exceptions.DataApiException:
			status['items_updated'] = False
			status['items_inserted'] = 0
			status['items_errors'].append("Data API TimeOUT exception")
		except OSError as e:
			print("OSError", e)
			# import pdb;pdb.set_trace()
			status['items_updated'] = False
			status['items_inserted'] = 0
			status['items_errors'].append("Connection Error")
		try:
			if(len(new_item_updates)>0):
				try:
					insert_gracefully(inteldb.item_collection, new_item_updates, ordered=True)
					print("ITEMS WORKED", len(new_item_updates))
					status['items_updated'] = True
					status['items_inserted'] = len(new_item_updates)
				except pymongo.errors.BulkWriteError as bwe:
					status['items_updated'] = False
					status['items_inserted'] = 0
					status['items_errors'].append(bwe.details)
			else:
				status['items_updated'] = True
				status['items_inserted'] = len(new_item_updates)
				status['items_errors'].append("No New Items Today")
		except UnboundLocalError:
			pass
	if(last_sync_obj['update_activities']):
	# Insert New Items into db; If successful, update status. 
		try:
			new_activity_updates, new_node_updates = update_activities(directory, df_activities, df_node_schema, df_tags, last_sync_obj['activities'])
			print("ACTIVITIES FETCHED", len(new_activity_updates))
		except learnosity_sdk.exceptions.DataApiException:
			print("learnosity_sdk.exceptions.DataApiException")
			status['activities_updated'] = False
			status['activities_inserted'] = 0
			status['activities_errors'].append("Connection Error exception")
		except OSError as e:
			print("OSError", e)
			# import pdb;pdb.set_trace()
			status['activities_updated'] = False
			status['activities_inserted'] = 0
			status['activities_errors'].append("Data API TimeOUT exception")
	# Insert New Activities into db; If successful, update status.
		try:
			if(len(new_activity_updates)>0):
				try:
					insert_gracefully(inteldb.activity_collection, new_activity_updates, ordered=True)
					insert_gracefully(inteldb.node_collection, new_node_updates, ordered=True)
					print("ACTIVITIES WORKED", len(new_activity_updates))
					status['activities_updated'] = True
					status['activities_inserted'] = len(new_activity_updates)
				except pymongo.errors.BulkWriteError as bwe:
					print("ACTIVITIES ERROR", len(new_activity_updates))
					status['activities_updated'] = False
					status['activities_inserted'] = 0
					status['activities_errors'].append(bwe.details)
			else:
				status['activities_updated'] = True
				status['activities_inserted'] = len(new_activity_updates)
				status['nodes_inserted'] = len(new_node_updates)
				status['activities_errors'].append("No New Activities Today")
		except UnboundLocalError:
			pass
	update_student_node_data(df_student_nodes, df_activities, df_student_chapters, df_tags)
	if(last_sync_obj['update_sessions']):
			# student_collection.bulk_write(student_worksheet_bulk_update_list, ordered = False)
		generator_retrieve_learnosity_session_data = retrieve_learnosity_session_data(inteldb, directory, df_activities, df_student_nodes, df_student_attempts, df_node_schema, last_sync_obj['sessions'])
		toggle_all_sessions_inserted = True
		while toggle_all_sessions_inserted:
			try:
				print("Starting Session Fetching")
				# new_sessions, new_item_responses = retrieve_learnosity_session_data(directory, df_activities, df_student_worksheet, df_student_attempts, last_sync_obj['sessions'])
				new_session_updates, new_item_response_updates, num_failed_sessions = next(generator_retrieve_learnosity_session_data)
				print("Did Session Fetching", len(new_session_updates), len(new_item_response_updates))
			except learnosity_sdk.exceptions.DataApiException:
				print("DATAAPI EXCEPTION")
				status['sessions_updated'] = False
				status['sessions_inserted'] = 0
				status['sessions_errors'].append("Data API TimeOUT exception")
				status['item_responses_updated'] = False
				status['item_responses_inserted']=0
				status['item_responses_errors'].append("Data API TimeOUT exception")
				time.sleep(200)
			except StopIteration:
				toggle_all_sessions_inserted = False
				print("Ending Session Fetching", len(new_session_updates), len(new_item_response_updates))
				try:
					if(num_failed_sessions!= -1):
						status['sessions_errors'].append(str(num_failed_sessions) + ' sessions failed today')
					pass
				except UnboundLocalError:
					num_failed_sessions = 0
				#new_sessions = 0
			# Insert New Sessions, item_responses into db, update status' if successful
			#print("Last set of sessionsL")
			try:
				if(len(new_session_updates)>0):
					# for j in range(1, int(math.ceil(len(new_sessions)/write_batch_size))+1):
						# new_sessions_batch = new_sessions[(write_batch_size*(j-1)):(write_batch_size*j)]
					try:
						insert_gracefully(inteldb.session_collection, new_session_updates, ordered=True)
						status['sessions_updated'] = True
						status['sessions_inserted'] += len(new_session_updates)
					except pymongo.errors.BulkWriteError as bwe:
						print("WRITE EXCEPTION")
						status['sessions_errors'].append(bwe.details)
						status['sessions_updated'] = False
						#status['sessions_inserted'] = len(new_sessions) - len(bwe.details['writeErrors'])
				else:
					status['sessions_errors'].append("No New Sessions Today")
				if(len(new_item_response_updates)>0):
					#for j in range(1, int(math.ceil(len(new_item_responses)/write_batch_size))+1):
					#	new_item_responses_batch = new_item_responses[(write_batch_size*(j-1)):(write_batch_size*j)]
					try:
						insert_gracefully(inteldb.item_response_collection, new_item_response_updates, ordered=True)
						status['item_responses_updated'] = True
						status['item_responses_inserted'] += len(new_item_response_updates)
					except pymongo.errors.BulkWriteError as bwe:
						print("WRITE EXCEPTION")
						status['item_responses_errors'].append(bwe.details)
						status['item_responses_updated'] = False
						#status['item_responses_inserted'] +=0
				else:
					status['item_responses_updated'] = True
					status['item_responses_inserted'] = 0
					status['item_responses_errors'].append("No New Item Responses Today")
			except UnboundLocalError:
				pass
	#print("SESSIONS WORKED", len(new_sessions), len(new_item_responses))
	status['dt_sync_completed'] = datetime.datetime.now()
	if(status['items_updated'] and status['activities_updated'] \
		and status['sessions_updated'] and status['item_responses_updated'] \
		and status['classrooms_updated'] and status['students_updated'] and status['teachers_updated']):
		status['sync_completed'] = True
	else:
		status['sync_completed'] = False
	#print(status)
	return(status)	#Depending on this status the config script would decide to change the last_sync details.

if __name__ == '__main__':
	direc = "./Trial2/"
	last_sync_default_obj={'items':default_date, 'activities':default_date, 'sessions':default_date, 'students':default_date, 'teachers':default_date, 'classrooms':default_date, \
	'update_items':False, 'update_activities':False, 'update_sessions':True, 'update_students': False, 'update_teachers': False, 'update_classrooms': False}
	db_instance_nm = sys.argv[1]
	assert db_instance_nm == 'local' or db_instance_nm == 'dev' or db_instance_nm == 'prod'
	update_all_dbs(direc, last_sync_default_obj, db_instance_nm)


'''
### TODO Changes ###
1) Add Connection Lag to Classroom AV Data
2) Get Date Due data and add Tags while activity creation.
3) Allot Primary Teachers and Secondary teachers to student.
4)
'''
