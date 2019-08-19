import psycopg2
import pandas as pd
import cuemath_db as cuedb
import config_update_script as cnfg
import math
import datetime
import uuid
import pymongo
from numpy import argsort
import pytz
import json


local_tz = pytz.timezone('Asia/Kolkata')

# client_mongo_prod = pymongo.MongoClient(cuedb.db_asli_intel_prod['host'], username = cuedb.db_asli_intel_prod['username'], password=cuedb.db_asli_intel_prod['password'], authSource='admin')
# inteldb = client_mongo_prod[cuedb.db_asli_intel_prod['db_name']]
# client_mongo = pymongo.MongoClient(cuedb.db_asli_intel_dev['host'])
# inteldb = client_mongo[cuedb.db_asli_intel_dev['db_name']]
class IntelDB:
    def __init__(self, db_type):
        if(db_type == 'dev'):
            client_mongo = pymongo.MongoClient(cuedb.db_asli_intel_dev['host'])
            inteldb = client_mongo[cuedb.db_asli_intel_dev['db_name']]
        elif(db_type == 'local'):
            client_mongo = pymongo.MongoClient(cuedb.db_asli_intel['host'])
            inteldb = client_mongo[cuedb.db_asli_intel['db_name']]
        elif(db_type == 'prod'):
            client_mongo_prod = pymongo.MongoClient(cuedb.db_asli_intel_prod['host'], username = cuedb.db_asli_intel_prod['username'], password=cuedb.db_asli_intel_prod['password'], authSource='admin')
            inteldb = client_mongo_prod[cuedb.db_asli_intel_prod['db_name']]
        self.item_collection = inteldb['Items']
        self.activity_collection = inteldb['Activities']
        self.item_response_collection = inteldb['Item_Responses']
        self.session_collection = inteldb['Sessions']
        self.student_collection = inteldb['Students']
        self.program_collection = inteldb['Programs']
        self.chapter_collection = inteldb['Chapters']
        self.block_collection = inteldb['Blocks']
        self.teacher_collection = inteldb['Teachers']
        self.classroom_collection = inteldb['Classrooms']
        self.schedules_collection = inteldb['Schedules']
        self.node_collection = inteldb['Nodes']
    @staticmethod
    def utc_to_local(utc_dt):
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        return local_tz.normalize(local_dt)

    @staticmethod
    def get_worksheet_db_data():
        #Get Data from Cuemath's Intel Database
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        cols = ["id", "name", "course_type",
                "created_on", "updated_on", "code",
                "learnosity_activity_ref", "target_accuracy",
                "description", "total_questions", "meta_data",
                "points_available", "sheet_time",
                "complete_in", "is_live"]
        sql_select = "SELECT "+", ".join(cols)+" FROM fnrenames.service_concepts_cue_worksheet;"
        #print(sql_select)
        cur.execute(sql_select)
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        cur.close()
        conn.close()
        return(df)
    '''
    def get_worksheet_tag_data():
        # Get Worksheet Tag data
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()

        cur.execute(sql_select)
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        cur.close()
        conn.close()
        return(df)
    '''
    @staticmethod
    def get_all_chapter_list():
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        sql_query = "select distinct(chp.code), prg.code, chp.name from fnrenames.service_concepts_cue_chapter chp" \
                     + " inner join fnrenames.service_concepts_cue_program prg on (chp.program_id = prg.id);"
        cur.execute(sql_query)
        df = pd.DataFrame(cur.fetchall(), columns = ['chapter_code', 'program_code', 'chapter_name'])
        cur.close()
        conn.close()
        return(df)

    @staticmethod
    def get_all_program_list():
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        sql_query = "select id, name, code, description, is_live, grade, created_on from fnrenames.service_concepts_cue_program;"
        cur.execute(sql_query)
        df = pd.DataFrame(cur.fetchall(), columns = ['id', 'name', 'code', 'description', 'is_live', 'grade', 'created_on'])
        cur.close()
        conn.close()
        return(df)


    @staticmethod
    def get_node_schema():
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        node_cols = ["id", "created_on", "updated_on", "chapter_id",
                    "worksheet_id", "course_type", "is_live",
                    "is_timed", "complete_in", "sheet_time",
                    "points_available", "complete_criteria",
                    "reattempt_criteria", "unlock_criteria",
                    "position", "attempt_location"
                    ]
        node_cols = ["node."+ w for w in node_cols]
        chapter_cols = ["id", "name", "code", "description",
                    "is_default", "position", "program_id", "version", "is_live",
                    "latest_chapter_id"]
        chapter_cols = ['chapter.'+ c for c in chapter_cols]
        program_cols = ["id", "name", "code", "description", "grade", "is_live"]
        program_cols = ['program.'+p for p in program_cols]
        sql_select = "SELECT " + ", ".join(node_cols) \
                    + ", " + ", ".join(chapter_cols) \
                    + ", " + ", ".join(program_cols) \
                    + " FROM fnrenames.service_concepts_cue_node node LEFT JOIN fnrenames.service_concepts_cue_chapter chapter on (node.chapter_id = chapter.id)" \
                    + " LEFT JOIN fnrenames.service_concepts_cue_program program on (chapter.program_id = program.id)";
        cur.execute(sql_select)
        df = pd.DataFrame(cur.fetchall(), columns = node_cols + chapter_cols + program_cols )
        cur.close()
        conn.close()
        return(df)

    @staticmethod
    def get_tags_data():
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        entity_tag_cols = ["entity_id", "tag_id", "entity_type"]
        entity_tag_cols = ['ent.' + et for et in entity_tag_cols]
        tag_cols = ["id", "meta_data", "type", "code", "name"]
        tag_cols = ['tg.' + tg for tg in tag_cols]
        sql_query = "SELECT "+ ", ".join(entity_tag_cols) \
                            + ", " + ", ".join(tag_cols) \
                            + " FROM fnrenames.service_concepts_cue_entity_tag ent" \
                            + " INNER JOIN fnrenames.service_concepts_cue_tag tg on (ent.tag_id = tg.id);"
        cur.execute(sql_query)
        df_remedials = pd.DataFrame(cur.fetchall(), columns = entity_tag_cols + tag_cols)
        cur.close()
        conn.close()
        return(df_remedials)

    @staticmethod
    def get_student_data(student_list):
        '''
        Gets Data of Students Enrolled under the Cue_User db. 
        '''
        conn_auth = cuedb.get_cuemath_redshift_connection()
        cur_auth = conn_auth.cursor()
        # Get User from cue_user table. Check email ID; Return True and False
        cols = ['id', 'created_on', 'updated_on', 'email', 'phone', 'first_name', 'last_name', 'gender', 'username']
        sql_student_query = "SELECT "+", ".join(cols)+" FROM service_auth_cue_user WHERE is_active='true' AND id=ANY(%s);"
        cur_auth.execute(sql_student_query, [student_list,])
        student_dat = pd.DataFrame(cur_auth.fetchall(), columns = cols)
        cur_auth.close()
        conn_auth.close()
        if(student_dat.shape[0]==0):
            raise Exception("No Students in DB")
        else:
            return(student_dat)

    @staticmethod
    def get_student_program_data():
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        sql_query = "SELECT std_prog.program_id, std_prog.user_id, program.code, program.description, program.grade, program.is_live, std_prog.started_on, std_prog.completed_on \
                     FROM fnrenames.service_concepts_user_entity std_prog inner join fnrenames.service_concepts_cue_program program on (std_prog.program_id = program.id)\
                     WhERE std_prog.entity_type = 'PROGRAM' and program.is_live='true';"
        cur_intel.execute(sql_query)
        df_student_program = pd.DataFrame(cur_intel.fetchall(), columns=['program_id', 'user_id', 'program_code', 'program_description', 'grade', 'is_live', 'started_on', 'completed_on'])
        cur_intel.close()
        conn_intel.close()
        return(df_student_program)


    @staticmethod
    def get_student_chapter_data(last_sync_dt):
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        # Add chapter.expected_time_complete, chapter.,made_live_on,
        sql_query = "SELECT std_chap.program_id, program.code, program.name, std_chap.entity_id, std_chap.entity_type, std_chap.user_id, std_chap.created_on, \
                        chapter.code, chapter.name, chapter.position, chapter.version, std_chap.started_on, std_chap.completed_on \
                     FROM fnrenames.service_concepts_user_entity std_chap left join fnrenames.service_concepts_cue_chapter chapter \
                     on (std_chap.entity_id = chapter.id) left join fnrenames.service_concepts_cue_program program \
                     on (std_chap.entity_id = program.id) \
                     WHERE ((std_chap.entity_type = 'CHAPTER' and chapter.is_live='true') or std_chap.entity_type = 'PROGRAM'\
                      and std_chap.updated_on > date(%s));"
        cur_intel.execute(sql_query, [last_sync_dt, ])
        df_student_program = pd.DataFrame(cur_intel.fetchall(), columns=['program_id', 'program_code', 'program_name', 'entity_id', 'entity_type', 'user_id', 'created_on',
                                                                         'chapter_code', 'chapter_name', 'chapter_position', 
                                                                         'chapter_version', 'started_on', 'completed_on'])
        cur_intel.close()
        conn_intel.close()
        return(df_student_program)

    @staticmethod
    def get_student_node_data(last_sync_dt):
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()

        student_worksheet_cols = ['program_id', 
                                    'worksheet_id', 'node_id', 'user_id', 'created_on', 
                                    'updated_on', 'points_earned', 'started_on', 'last_attempted_on', 
                                    'completed_on', 'attempt_location', 'is_timed', 'course_type', "is_live", 
                                    'is_attempt_in_progress', 'complete_status', 'reattempt_status',
                                    'unlock_status', 'last_attempt_id']
        student_worksheet_cols_qry =['std_nd.'+ col for col in student_worksheet_cols]
        sql_query = "SELECT "+", ".join(student_worksheet_cols_qry)+", uent.entity_id FROM fnrenames.service_concepts_user_node std_nd LEFT JOIN fnrenames.service_concepts_user_entity uent ON (uent.id=std_nd.user_chapter_id) WHERE std_nd.updated_on > date(%s)-2;"
        cur_intel.execute(sql_query, [last_sync_dt,])
        student_worksheet_cols.append('chapter_id')
        df_student_worksheet = pd.DataFrame(cur_intel.fetchall(), columns=student_worksheet_cols)
        cur_intel.close()
        conn_intel.close()
        return(df_student_worksheet)

    @staticmethod
    def get_student_attempts_data(last_sync_dt):
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        student_attempt_cols = ["id", "worksheet_id", "node_id", "user_id", "started_on", "completed_on",
                                "last_attempted_on", "points_earned", "accuracy", "learnosity_activity_ref"]
        sql_query = "SELECT "+", ".join(student_attempt_cols)+" FROM fnrenames.service_concepts_user_attempt WHERE updated_on > date(%s)-2"; 
        cur_intel.execute(sql_query, [last_sync_dt,])
        df_student_attempts = pd.DataFrame(cur_intel.fetchall(), columns=student_attempt_cols)
        cur_intel.close()
        conn_intel.close()
        return(df_student_attempts)

    @staticmethod
    def get_student_attempts_data_by_attempt_id(session_id):
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        student_attempt_cols = ["id", "worksheet_id", "node_id", "user_id", "started_on", "completed_on", 
                                "last_attempted_on", "points_earned", "accuracy", ]
        sql_query = "SELECT "+", ".join(student_attempt_cols)+" FROM fnrenames.service_concepts_user_attempt WHERE id=(%s)"; 
        cur_intel.execute(sql_query, [session_id,])
        df_student_attempts = pd.DataFrame(cur_intel.fetchall(), columns=student_attempt_cols)
        cur_intel.close()
        conn_intel.close()
        return(df_student_attempts)

    @staticmethod
    def get_schedules():
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        sql_query = "SELECT id, created_on, updated_on, meta, schedule_code FROM service_classroom_schedules;"
        cur_intel.execute(sql_query)
        df_schedules = pd.DataFrame(cur_intel.fetchall(), columns = ["id", "created_on", "updated_on", "meta", "schedule_code"])
        cur_intel.close()
        conn_intel.close()
        return(df_schedules)

    @staticmethod
    def get_student_slots():
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        student_slot_cols = ["id", "created_on", "updated_on", "student_id", "is_demo", "_status", "_type"]
        teacher_slot_cols = ["id", "created_on", "updated_on", "teacher_id", "schedule_id", "grades", "duration", "is_demo", "start_date", "end_date"]
        sql_query = "SELECT " + ", ".join(["service_classroom_student_slots."+ w for w in student_slot_cols]) \
                            + ", " + ", ".join(["service_classroom_teacher_slots."+ w for w in teacher_slot_cols]) \
                            + " FROM service_classroom_student_slots left join service_classroom_teacher_slots on \
                            (service_classroom_student_slots.teacher_slot_id = service_classroom_teacher_slots.id);"
        #print(sql_query)
        cur_intel.execute(sql_query)
        df_student_slots = pd.DataFrame(cur_intel.fetchall(), columns= ["student_slots_"+ w for w in student_slot_cols] \
                                                                +["teacher_slots_" + b for b in teacher_slot_cols])
        cur_intel.close()
        conn_intel.close()
        return(df_student_slots)

    @staticmethod
    def get_all_teachers():
        '''
        Save all Teachers Information; Edit info when reqd. 
        '''
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        teacher_cols = ["id", "created_on", "updated_on", "meta", "first_name", "last_name", "gender", "phone", "email", "is_demo"]
        sql_query = "SELECT " + ", ".join(teacher_cols) + " FROM service_teacher_teacher;"
        cur.execute(sql_query)
        df_teachers = pd.DataFrame(cur.fetchall(), columns=teacher_cols)
        return(df_teachers)

    @staticmethod
    def get_teacher_slots():
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        teacher_slot_cols = ["id", "created_on", "updated_on", "teacher_id", "schedule_id", "grades", "duration", "is_demo", "_status", "_type"]
        sql_query = "SELECT " + ", ".join(teacher_slot_cols) + " FROM service_classroom_teacher_slots;"
        cur_intel.execute(sql_query)
        df_teacher_slots = pd.DataFrame(cur_intel.fetchall(), columns = teacher_slot_cols)
        cur_intel.close()
        conn_intel.close()
        return(df_teacher_slots)

    @staticmethod
    def get_primary_slots_and_teacher(df_student_slots):
        dummy_teacher_ids = ["fa1e5154-1fb9-11e9-a2c9-02420a0001da", "8f19e370-d1ec-11e8-a0a7-02420a00002f"]
        relevant_slots = df_student_slots[(df_student_slots['teacher_slots_is_demo']=='false')\
                                         & (df_student_slots['student_slots__type']=='REG')\
                                         & (df_student_slots['student_slots__status']=='ACT')\
                                         & ~(df_student_slots['teacher_slots_teacher_id'].isin(dummy_teacher_ids))]
        if(relevant_slots.shape[0] == 0):
            return(None)
        #TODO: Find better heuristic
        relevant_slots_2 = relevant_slots['teacher_slots_teacher_id'].value_counts().to_dict()
        primary_teacher_id = max(relevant_slots_2, key=lambda k: relevant_slots_2[k])
        # import pdb;
        # pdb.set_trace()
        return(relevant_slots[relevant_slots['teacher_slots_teacher_id']==primary_teacher_id].iloc[0])

    @staticmethod
    def get_classroom_data(last_sync_dt):
        '''
        Save all classroom data; Classrooms are empty mostly, initially. They will be populated as session fetching happens.
        Get all student classrooms and find respective teacher classroom. Save schedule, student, teacher, class_timings_data.
        '''
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        student_classroom_cols = ["id", "student_id", "date", "scheduled_start_time", "scheduled_end_time", "enter_time", "exit_time", "_status"]
        teacher_classroom_cols = ["id", "teacher_id", "date", "scheduled_start_time", "scheduled_end_time", "attendance", "enter_time", "exit_time", "_status"]
        sql_query = "SELECT " + ", ".join(["service_classroom_student_classroom."+ w for w in student_classroom_cols]) \
                            + ", " + ", ".join(["service_classroom_teacher_classroom."+ w for w in teacher_classroom_cols]) \
                            + " FROM service_classroom_student_classroom left join service_classroom_teacher_classroom on \
                            (service_classroom_student_classroom.teacher_classroom_id = service_classroom_teacher_classroom.id)\
                             WHERE (date(service_classroom_student_classroom.created_on) >= (%s));"
        cur_intel.execute(sql_query, [last_sync_dt,])
        df_student_classrooms = pd.DataFrame(cur_intel.fetchall(), columns= ["student_classroom_"+ w for w in student_classroom_cols] \
                                                                +["teacher_classroom_" + b for b in teacher_classroom_cols])
        cur_intel.close()
        conn_intel.close()
        return(df_student_classrooms)

    @staticmethod
    def get_classroom_ratings_data(last_sync_dt):
        conn_intel = cuedb.get_cuemath_redshift_connection()
        cur_intel = conn_intel.cursor()
        classroom_ratings_cols = ['id', 'meta', 'created_on', 'rating', 'student_classroom_id']
        sql_query = "SELECT "+ ", ".join(classroom_ratings_cols) + " FROM fnrenames.service_classroom_classroom_rating WHERE DATE(created_on) > DATE(%s);"
        cur_intel.execute(sql_query, [last_sync_dt,])
        df_student_classroom_ratings = pd.DataFrame(cur_intel.fetchall(), columns = classroom_ratings_cols)
        cur_intel.close()
        conn_intel.close()
        return(df_student_classroom_ratings)

    @staticmethod
    def get_av_data(dt, user_id):
        '''
        Fetches all AV Related data for a student on a particular day
        '''

        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        av_monitor_cols = ["id", "created_on", "updated_on", "meta", "session_id", "event", "_activity_data"]
        # Next Query is bombastic
        sql_av_monitor = "SELECT "+ ", ".join(av_monitor_cols)+" FROM service_communication_avmonitoring where date(updated_on) = (%s) and (event='streamDestroyed' or event='streamCreated') and _activity_data::jsonb @> \'{\"stream\":{\"connection\":{\"data\":\"{\\\"userId\\\": \\\""+ user_id + "\\\", \\\"userRole\\\": \\\"publisher\\\"}\"}}}\'::jsonb;"
        cur.execute(sql_av_monitor, [dt,])
        df_av_monitor = pd.DataFrame(cur.fetchall(), columns = av_monitor_cols).sort_values('created_on')
        av_request_cols = ["id", "created_on", "updated_on", "meta", "requested_by", "requested_on", "is_handled"]
        sql_av_requests = "SELECT "+ ", ".join(av_request_cols)+" FROM service_communication_avrequest where DATE(requested_on) = (%s) and requested_by=(%s);"
        cur.execute(sql_av_requests, [dt, user_id])
        df_av_request = pd.DataFrame(cur.fetchall(), columns = av_request_cols)
        av_whiteboard_cols = ["id", "created_on", "updated_on", "ref_id", "image_url", "user_id"]
        sql_av_whiteboard = "SELECT "+ ", ".join(av_whiteboard_cols)+" FROM service_communication_whiteboardexport where date(updated_on) = (%s) and user_id=(%s);"
        cur.execute(sql_av_whiteboard, [dt, user_id])
        df_av_whiteboard = pd.DataFrame(cur.fetchall(), columns = av_whiteboard_cols).sort_values('created_on')
        cur.close()
        conn.close()
        return(df_av_monitor, df_av_request, df_av_whiteboard)

    @staticmethod
    def get_av_dict_user(x):
        try:
            stream_obj = json.loads(x)
            connection = stream_obj['stream']['connection']
            if(connection is not None and connection['data'] is not None):
                val = json.loads(connection['data'])
            else:
                raise ValueError
            # print("DEBUG_AV_REQUEST", type(x), type(stream_obj), type(connection), type(val))
            ret_x = (str(val['userId']), str(val['userRole']))
        except TypeError:
            raise ValueError
        except ValueError:
            ret_x = (None, None)
        return(ret_x)

    @staticmethod
    def get_av_data_by_date(dt):
        '''
        Fetches all AV Related data for a student on a particular day
        '''
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        av_monitor_cols = ["id", "created_on", "updated_on", "meta", "session_id", "event", "_activity_data"]
        # Next Query is bombastic
        sql_av_monitor = "SELECT "+ ", ".join(av_monitor_cols)+" FROM service_communication_avmonitoring where date(created_on) = (%s) and (event='streamDestroyed' or event='streamCreated');"
        '''
        sql_av_monitor = "select " + ", ".join(av_monitor_cols)+ ", "+ \
            " JSON_EXTRACT_PATH_TEXT(regexp_replace(JSON_EXTRACT_PATH_TEXT(_activity_data, 'stream', 'connection', 'data', TRUE)::character varying, %s, %s), 'userId'),"+\
            " JSON_EXTRACT_PATH_TEXT(regexp_replace(JSON_EXTRACT_PATH_TEXT(_activity_data, 'stream', 'connection', 'data', TRUE)::character varying, %s, %s), 'userRole')"+\
            " from service_communication_avmonitoring"+\
            " where date(created_on) = (%s) and (event='streamCreated');"
        #print(sql_av_monitor)
        '''
        cur.execute(sql_av_monitor, [dt,])
        # av_monitor_cols_2 = av_monitor_cols + ['user_id', 'user_role']
        df_av_monitor = pd.DataFrame(cur.fetchall(), columns = av_monitor_cols)
        body_txt = [IntelDB.get_av_dict_user(x) for idx, x in enumerate(list(df_av_monitor['_activity_data']))]
        df_av_monitor["user_id"] = pd.Series([body_txt[i][0] for i in range(len(body_txt))])
        df_av_monitor["user_role"] = pd.Series([body_txt[i][1] for i in range(len(body_txt))])
        av_request_cols = ["id", "created_on", "updated_on", "meta", "requested_by", "requested_on", "is_handled"]
        sql_av_requests = "SELECT "+ ", ".join(av_request_cols)+" FROM service_communication_avrequest where date(created_on) = (%s);"
        cur.execute(sql_av_requests, [dt,])
        df_av_request = pd.DataFrame(cur.fetchall(), columns = av_request_cols)
        av_whiteboard_cols = ["id", "created_on", "updated_on", "ref_id", "image_url", "user_id"]
        sql_av_whiteboard = "SELECT "+ ", ".join(av_whiteboard_cols)+" FROM service_communication_whiteboardexport where date(created_on) = (%s);"
        cur.execute(sql_av_whiteboard, [dt,])
        df_av_whiteboard = pd.DataFrame(cur.fetchall(), columns = av_whiteboard_cols).sort_values('created_on')
        cur.close()
        conn.close()
        return(df_av_monitor, df_av_request, df_av_whiteboard)

    @staticmethod
    def get_video_log_data(session_ids):
        conn = cuedb.get_cuemath_redshift_connection()
        cur = conn.cursor()
        sql_query = "SELECT DISTINCT JSON_EXTRACT_PATH_TEXT(_activity_data,'id', TRUE)||JSON_EXTRACT_PATH_TEXT(_activity_data,'sessionId', TRUE), "+ \
            "'https://s3-ap-southeast-1.amazonaws.com/cuemath-tokbox/'||JSON_EXTRACT_PATH_TEXT(_activity_data,'projectId',TRUE)::character varying||'/'||JSON_EXTRACT_PATH_TEXT(_activity_data,'id',TRUE)::character varying||'/archive.mp4' " +\
            "from service_communication_avmonitoring where session_id=ANY(%s) and event='archive';"
        cur.execute(sql_query, [session_ids,])
        df_video_logs = pd.DataFrame(cur.fetchall(), columns = ["row", "video_url"])
        video_url_list = list(df_video_logs["video_url"])
        # print("DEBUG Videolog fetching: ", df_video_logs)
        cur.close()
        conn.close()
        return(video_url_list)


def retry(num_tries, exceptions):
    def decorator(func):
        def f_retry(*args, **kwargs):
            for i in range(num_tries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print("Exception ", e, "has occurred")
                    continue
        return f_retry
    return decorator
# Retry on AutoReconnect exception, maximum 3 times
retry_auto_reconnect = retry(10, (pymongo.errors.AutoReconnect,))
