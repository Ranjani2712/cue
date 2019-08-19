import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from update_from_cuedb import *
from curriculum_google_sheet import initialize_creds
from pprint import pprint
import numpy as np
import string
from dateutil.relativedelta import *
import math

fl_nm_pickle = './google_sheets/token.pickle'
spreadsheet_id = '1TxGoA8n_f-OLcMs9G5i5i4MXbfX0ZpqpDMDIy4zG1P8'
teacher_spreadsheet_id = '1OUkONFhHmRZ6SvGOngb4XyjLiLZB8Xmj00cFvwyRWDw'
inteldb = IntelDB('prod')
dt_start = datetime.date(2019, 7, 1)

alphabet_dict = dict(enumerate([x for x in string.ascii_uppercase] \
                             + ['A' + x for x in string.ascii_uppercase], 1))


def get_datetime(date):
    # print(type(date))
    if(type(date)==type('str')):
        return datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    else:
        return date - datetime.timedelta(hours=5, minutes=30)

def get_doubt_to_call_time(call_data, dbts_raised, class_start_dt_time, class_end_dt_time, cls_id):
    # Get list of time lapsed between doubt raised and call made.
    dbt_tmstmp_list = []
    dbt_tmstmp_list.append(class_start_dt_time)
    if(not dbts_raised or len(dbts_raised)==0):
        return([], 0)
    dbt_tmstmp_list += [get_datetime(dbt.get('raised_at').get('created_on')) for dbt in dbts_raised.values()]
    call_data_list = sorted(call_data, key=lambda x: x.get('start_time'), reverse=False)
    doubt_to_call_time_list = []
    num_unresolved_dbts = 0
    toggle_dbt_resolved = True
    for idx in range(len(dbt_tmstmp_list)-1):
        dbt_tm = dbt_tmstmp_list[idx]
        next_dbt_tm = dbt_tmstmp_list[idx+1]
        if(not toggle_dbt_resolved):
            num_unresolved_dbts += 1
        toggle_dbt_resolved = False
        for call in call_data_list:
            # print(type(call.get('start_time')), type(dbt_tm), type(next_dbt_tm))
            # print(dbt_tms, call.get('start_time'), next_dbt_tm)
            if(call.get('start_time') > dbt_tm and call.get('start_time') <= next_dbt_tm):
                doubt_to_call_time_list.append((call.get('start_time') - dbt_tm).total_seconds())
                toggle_dbt_resolved = True
                continue
    # print(doubt_to_call_time_list, num_unresolved_dbts)
    return(doubt_to_call_time_list, num_unresolved_dbts)

def get_classroom_metrics(dt_start_analysis, dt_end_analysis, student_teacher_dict):
    inteldb = IntelDB('prod')
    prg_list= ['G7', 'G8', 'CBSE.G9', 'CBSE.G10', 'ICSE.G9', 'ICSE.G10']
    students = inteldb.student_collection.collection.find({'current_program':{'$in':prg_list}, 'is_demo':False})
    student_id_list = []
    student_name_dict = {}
    total_classes = 0
    for std in students:
        student_name_dict[std.get('_id')] = {'name': std.get('first_name') + ' ' + std.get('last_name'),
                                            'phone':std.get('phone'),
                                            'dt_joining':std.get('created_on'),
                                            'current_program':std.get('current_program')}
        student_id_list.append(std.get('_id'))
    student_ids = list(set(student_id_list))
    classrooms = inteldb.classroom_collection.collection.find({
                                                            'student_id':{'$in':student_ids},
                                                            'date':{
                                                                '$gt': str(dt_start_analysis),
                                                                '$lte': str(dt_end_analysis)
                                                            }}).sort('date')
    teacher_id_lst = []
    classrooms_list = []
    num_students_per_class_list = {}
    for clsrm in classrooms:
        classrooms_list.append(clsrm)
    teachers = inteldb.teacher_collection.collection.find()
    teacher_name_dict = {}
    for tch in teachers:
        teacher_name_dict[tch.get('_id')] = tch.get('first_name') + ' ' + tch.get('last_name')
    student_dict = {}
    for clsrm in classrooms_list:
        # print("HEY", clsrm.get('_id'))
        if(not student_teacher_dict.get(clsrm.get('teacher_id'))):
            student_teacher_dict[clsrm.get('teacher_id')] = {}
        if(not student_teacher_dict[clsrm.get('teacher_id')].get(clsrm.get('student_id'))):
            student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')] = {
                'student_name':student_name_dict.get(clsrm.get('student_id')).get('name'),
                'teacher_name':teacher_name_dict.get(clsrm.get('teacher_id')),
                'student_dt_joined':student_name_dict.get(clsrm.get('student_id')).get('dt_joining'),
                'student_program':student_name_dict.get(clsrm.get('student_id')).get('current_program'),
                'student_phone':student_name_dict.get(clsrm.get('student_id')).get('phone'),
                'num_calls':0,
                'total_call_dur':0,
                'time_per_call':0,
                'doubt_to_call_time':0,
                'num_doubts':0,
                'num_dropped_calls':0,
                'num_classes':0,
                'num_sheets_attempted':0,
                'num_absent_classes':0,
                'num_items_attempted':0
                }
        if(not student_dict.get(clsrm.get('student_id'))):
            student_dict[clsrm.get('student_id')] = {}
            student_dict[clsrm.get('student_id')]['total_call_dur'] = []
            student_dict[clsrm.get('student_id')]['num_dropped_calls'] = []
            student_dict[clsrm.get('student_id')]['doubt_to_call_time'] = []
            student_dict[clsrm.get('student_id')]['num_calls'] = []
            student_dict[clsrm.get('student_id')]['num_doubts'] = []
            student_dict[clsrm.get('student_id')]['num_unresolved_dbts'] = []
            student_dict[clsrm.get('student_id')]['num_sheets_attempted'] = []
            student_dict[clsrm.get('student_id')]['num_items_attempted'] = []
            student_dict[clsrm.get('student_id')]['total_time_on_ws'] = []
            student_dict[clsrm.get('student_id')]['rating'] = []
            student_dict[clsrm.get('student_id')]['classes_attended'] = 0
            student_dict[clsrm.get('student_id')]['classes_total'] = 0
        av_data = clsrm.get('av_data')
        student_dict[clsrm.get('student_id')]['classes_total'] += 1
        num_students_per_class_list[clsrm.get('teacher_id')
                                    +'_'+clsrm.get('date')
                                    +'_'+clsrm.get('scheduled_start_time')
                                    +'_'+clsrm.get('scheduled_end_time')
                                    ] = clsrm.get('teacher_num_attendees')
        if(av_data is None):
            continue
        total_call_dur = 0.0
        total_connect_attempts = 0
        num_calls = 0
        total_stream_lag = 0.0
        nm_lg_available = 0
        total_time_on_ws = 0.0
        num_sheets_attempted = 0.0
        num_items_attempted = 0
        class_end_tm = datetime.datetime.strptime(clsrm['scheduled_end_time'], '%H:%M:%S').time()
        class_start_tm = datetime.datetime.strptime(clsrm['scheduled_start_time'], '%H:%M:%S').time()
        class_end_dt_time = datetime.datetime.combine(datetime.datetime.strptime(clsrm['date'], '%Y-%m-%d'), class_end_tm)
        class_start_dt_time = datetime.datetime.combine(datetime.datetime.strptime(clsrm['date'], '%Y-%m-%d'), class_start_tm)
        doubt_to_call_time_list, num_unresolved_dbts = get_doubt_to_call_time(av_data.get('call_data'), av_data.get('doubts_raised'), class_start_dt_time, class_end_dt_time, clsrm.get('_id'))
        pos_doubt_to_call_time_list = [x for x in doubt_to_call_time_list if x > 0]
        if(len(pos_doubt_to_call_time_list)>0):
            mean_doubt_to_call_time = sum(pos_doubt_to_call_time_list)/len(pos_doubt_to_call_time_list)
        else:
            mean_doubt_to_call_time = -1 #Need to choose a better constant.s
        for call in av_data.get('call_data', []):
            total_call_dur += float(call.get('duration_in_seconds'))/60.0
            num_calls += 1
            total_connect_attempts += call.get('connect_attempts')
            if(not pd.isnull(call.get('call_stream_lag')) and call.get('call_stream_lag')>0):
                total_stream_lag += call.get('call_stream_lag')
                nm_lg_available += 1

        num_doubts = sum([dbt.get('num_button_press') for dbt in av_data.get('doubts_raised', {}).values()])
        if(nm_lg_available == 0):
            avg_call_lag = 'NA'
        else:
            avg_call_lag = total_stream_lag/nm_lg_available
        for session_id, session in clsrm.get('sessions_attempted').items():
            num_items_attempted += session.get('num_attempted_today')
            num_sheets_attempted += session.get('completion')
            total_time_on_ws += session.get('time_spent')/60.0
        rt = clsrm.get('ratings', {})
        rating = rt.get('rating', 'NA')
        slot = clsrm.get('scheduled_start_time', '') + '-' + clsrm.get('scheduled_end_time', '')
        student_dict[clsrm.get('student_id')]['total_call_dur'].append(total_call_dur)
        student_dict[clsrm.get('student_id')]['num_dropped_calls'].append((av_data.get('dropped_calls', 0) + total_connect_attempts))
        student_dict[clsrm.get('student_id')]['doubt_to_call_time'].append(mean_doubt_to_call_time)
        student_dict[clsrm.get('student_id')]['num_unresolved_dbts'].append(num_unresolved_dbts)
        student_dict[clsrm.get('student_id')]['num_calls'].append(num_calls+total_connect_attempts)
        student_dict[clsrm.get('student_id')]['num_doubts'].append(num_doubts)
        student_dict[clsrm.get('student_id')]['num_sheets_attempted'].append(num_sheets_attempted)
        student_dict[clsrm.get('student_id')]['num_items_attempted'].append(num_items_attempted)
        student_dict[clsrm.get('student_id')]['total_time_on_ws'].append(total_time_on_ws)
        student_dict[clsrm.get('student_id')]['rating'].append(rating)
        if(clsrm.get('status')=='P' and num_sheets_attempted>0):
            student_dict[clsrm.get('student_id')]['classes_attended'] += 1
            student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_classes'] += 1
            total_classes += 1
        elif(clsrm.get('status')=='A'):
            student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_absent_classes'] += 1
        # if(clsrm.get('status')!='TA'):
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_calls'] += (num_calls+total_connect_attempts)
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_dropped_calls'] += (av_data.get('dropped_calls', 0) + total_connect_attempts)

        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['total_call_dur'] += total_call_dur
        if(num_calls!=0):
            student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['time_per_call'] += (float(total_call_dur)/num_calls)
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['doubt_to_call_time'] += mean_doubt_to_call_time/60.0
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_doubts'] += num_doubts
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_items_attempted'] += num_items_attempted
        student_teacher_dict[clsrm.get('teacher_id')][clsrm.get('student_id')]['num_sheets_attempted'] += num_sheets_attempted


    """
    Number of sheets completed per student per class
    % of student below avg. sheet completion of 2 per class
    # Tech Issues per student
    # Call Connectivity issues/ student
    # Whiteboard Issues/ student
    Avg. Internet Speed at the time of issues (Teacher|Student)
    NPS (Student): Rating
    NPS (Parent)
    # Avg. uncleared doubts per class
    Attandance %
    Avg. number of times teacher connect on AV call with student in a class
    Avg. time teacher is on call per student per class
    Median Time to Resolve Doubt
    Avg time b/w doubt raised and call made
    Home work completion % within due date
    Avg number of chapters completed in first 3|6|9 months by a student
    """
    num_sheets_per_class_list = []
    num_students_below_avg_completion_2 = 0
    num_dropped_calls = 0
    num_total_calls = 0
    rating_list = []
    doubt_to_call_time_list = []
    attendance_list = []
    median_num_calls_list = []
    mean_call_duration_list = []
    num_doubt_list = []
    median_num_items_attempted_list = []
    median_time_on_ws = []
    median_unresolved_doubts = []
    num_unresolved_dbts = 0
    median_doubts_list = []
    actual_student_id_list = []
    # output_metrics = []
    for student_id, metrics in student_dict.items():
        actual_student_id_list.append(student_id)
        if(len(metrics.get('num_sheets_attempted'))>0):
            mean_sheets = float(sum(metrics.get('num_sheets_attempted')))/len(metrics.get('num_sheets_attempted'))
            num_sheets_per_class_list.append(mean_sheets)
        if(mean_sheets<=2):
            num_students_below_avg_completion_2 += 1
        total_dropped_calls = sum(metrics.get('num_dropped_calls'))
        num_dropped_calls += total_dropped_calls
        num_total_calls += sum(metrics.get('num_calls'))
        non_na_ratings = [rt for rt in metrics.get('rating') if rt!='NA']
        if(len(non_na_ratings)>1):
            rating_list.append(np.percentile(non_na_ratings, 50))
        dbt_call_time, num = 0, 0
        for dbt_cll_tm in metrics.get('doubt_to_call_time'):
            if(dbt_cll_tm<3600 and dbt_cll_tm!=-1):
                dbt_call_time += dbt_cll_tm
                num += 1
        if(num>0):
            doubt_to_call_time_list.append(float(dbt_call_time)/num)
        if(metrics.get('classes_total')>0):
            # print(metrics.get('classes_attended'), metrics.get('classes_total'))
            attendance_list.append(float(metrics.get('classes_attended'))/metrics.get('classes_total'))
        if(len(metrics.get('total_call_dur'))>0 and len(metrics.get('num_calls'))>0):
            median_num_calls = np.percentile(metrics.get('num_calls'), 50)
            median_num_calls_list.append(median_num_calls)
            # if(sum(metrics.get('num_calls'))>0):
            mean_dur_calls = sum(metrics.get('total_call_dur'))/len(metrics.get('total_call_dur'))
            mean_call_duration_list.append(mean_dur_calls)
        if(len(metrics.get('num_doubts'))>0):
            num_doubt_list.append(sum(metrics.get('num_doubts')))
            median_doubts_list.append(float(sum(metrics.get('num_doubts')))/len(metrics.get('num_doubts')))
        if(len(metrics.get('num_items_attempted'))>1):
            median_num_items_attempted_list.append(np.percentile(metrics.get('num_items_attempted'), 50))
        if(len(metrics.get('total_time_on_ws'))>1):
            median_time_on_ws.append(np.percentile(metrics.get('total_time_on_ws'), 50))
        if(len(metrics.get('num_unresolved_dbts'))>1):
            median_unresolved_doubts.append(np.percentile(metrics.get('num_unresolved_dbts'), 50))
            num_unresolved_dbts += sum(metrics.get('num_unresolved_dbts'))
    if(len(rating_list)>0):
        mean_rating = sum(rating_list)/len(rating_list)
    else:
        mean_rating = 'NA'
    if(len(median_unresolved_doubts)>0):
        avg_unresolved_dbts = sum(median_unresolved_doubts)/len(median_unresolved_doubts)
    else:
        avg_unresolved_dbts= 'NA'
    if(len(attendance_list)>1):
        attendance = float(sum(attendance_list))/len(attendance_list)
    else:
        attendance = 'NA'
    if(len(median_num_calls_list) >0):
        median_num_calls = sum(median_num_calls_list)/len(median_num_calls_list)
    else:
        median_num_calls= 'NA'
    if(len(mean_call_duration_list)>0):
        mean_call_duration = sum(mean_call_duration_list)/len(mean_call_duration_list)
    else:
        mean_call_duration = 'NA'
    pos_doubt_to_call_time_list = [x for x in doubt_to_call_time_list if x >0]
    if(len(pos_doubt_to_call_time_list)>1):
        dbt_call_metric = np.percentile(pos_doubt_to_call_time_list, 50)
    else:
        dbt_call_metric = 'NA'
    num_students_per_class_total = 0
    num_classes_unique = 0
    if(num_total_calls>0):
        frac_dropped_calls = num_dropped_calls/num_total_calls
    else:
        frac_dropped_calls = 'NA'
    if(sum(median_doubts_list) > 0):
        tm_per_dbt = (mean_call_duration*len(median_doubts_list)/sum(median_doubts_list))
    else:
        tm_per_dbt = 'NA'
    for k, v in num_students_per_class_list.items():
        num_students_per_class_total += v
        num_classes_unique += 1
    num_students_per_class = num_students_per_class_total/num_classes_unique
    output_metrics = [  sum(num_sheets_per_class_list)/len(num_sheets_per_class_list),
                        len(actual_student_id_list),
                        num_students_below_avg_completion_2,
                        total_classes,
                        num_dropped_calls,
                        num_total_calls,
                        frac_dropped_calls,
                        'NA',
                        mean_rating,
                        'NA',
                        avg_unresolved_dbts,
                        num_students_per_class,
                        attendance,
                        median_num_calls,
                        mean_call_duration,
                        tm_per_dbt,
                        dbt_call_metric,
                        sum(num_doubt_list)-num_unresolved_dbts,
                        'NA',
                        'NA',
                        np.percentile(median_num_items_attempted_list, 50),
                        np.percentile(median_time_on_ws, 50),
                    ]
    # print(student_teacher_dict)
    return([[ot] for ot in output_metrics], student_teacher_dict)


def main():
    #Get Start of week, end of week;
    #Get data.
    #Send to column x+1 where x is week x;
    num_months_back = 10
    today = datetime.datetime.today().date()
    # dt_start = today - relativedelta(months=+nm_weeks_back) #datetime.timedelta(days=today.weekday(), months = nm_weeks_back)
    dt_start = datetime.date(2018, 10, 1)
    print("Started on", dt_start)
    student_teacher_dict = {}
    creds = initialize_creds()
    service = build('sheets', 'v4', credentials=creds)
    for nm_mth in range(num_months_back):
        dt_initial = dt_start + relativedelta(months=+nm_mth)
        dt_final = dt_start + relativedelta(months=+nm_mth+1)
        # week_num = (dt_initial - dt_start).days/
        if(dt_final > today):
            dt_final = today
            sheet_nm_wk = 'SheetWeek'
            nm_weeks_back = int(math.ceil((dt_final - dt_initial).days/7))
            student_teacher_dict_wk = {}
            for nm_wk in range(nm_weeks_back, 0, -1):
                dt_init = today - datetime.timedelta(days=today.weekday(), weeks = nm_wk)
                dt_fin = today - datetime.timedelta(days=today.weekday(), weeks = nm_wk-1)
                week_num = int((dt_init - dt_initial).days/7)
                dat, student_teacher_dict_wk = get_classroom_metrics(str(dt_init), str(dt_fin), student_teacher_dict_wk)
                print("TEST WK", str(dt_init), str(dt_fin), week_num)
                final_request_wk = {
                    "value_input_option": "USER_ENTERED",
                    "data": [{
                            "range": sheet_nm_wk +"!"+str(alphabet_dict[week_num+3])+"2:"+str(alphabet_dict[week_num+3])+str(len(dat)+3),
                            "values": dat
                        }]
                    }
                request = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=final_request_wk)
                response = request.execute()
                print('Response', response)
        print("TEST", str(dt_initial), str(dt_final), nm_mth)
        data, student_teacher_dict = get_classroom_metrics(str(dt_initial), str(dt_final), student_teacher_dict)
        sheet_nm = 'SheetMonth'
        final_request = {
            "value_input_option": "USER_ENTERED",
            "data": [{
                    "range": sheet_nm +"!"+str(alphabet_dict[nm_mth+3])+"2:"+str(alphabet_dict[nm_mth+3])+str(len(data)+3),
                    "values": data
                }]
            }
        request = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=final_request)
        response = request.execute()
        print('Response', response)
    output_agg = [['Teacher Name',
                'Student Name',
                'Parent Phone Number',
                'Current Program',
                'Date of Joining',
                'Number of Classes Attended',
                'Number of Classes Missed',
                'Number of Calls',
                'Number of Dropped Calls',
                'Total Call Duration',
                'Call Duration per Class',
                'Time per Call',
                'Mean Doubt to Call Time',
                'Total Number of Doubts',
                '# Worksheets Attempted',
                '# Items Attempted',
                'Link'
                ]]
    for teacher_id, teacher_dict in student_teacher_dict.items():
            for student_id, agg_obj in teacher_dict.items():
                if(agg_obj['doubt_to_call_time']>0):
                    mean_dbt_call = agg_obj['doubt_to_call_time']
                else:
                    mean_dbt_call='NA'
                if(agg_obj['num_classes'] > 0):
                    avg_call_dur = agg_obj['total_call_dur']/agg_obj['num_classes']
                else:
                    avg_call_dur = 'NA'
                student_url="https://leap-teacher.cuemath.com/student-profile/"+student_id+"/analytics"
                output_agg.append([
                    agg_obj['teacher_name'],
                    agg_obj['student_name'],
                    agg_obj['student_phone'],
                    agg_obj['student_program'],
                    str(agg_obj['student_dt_joined']),
                    agg_obj['num_classes'],
                    agg_obj['num_absent_classes'],
                    agg_obj['num_calls'],
                    agg_obj['num_dropped_calls'],
                    agg_obj['total_call_dur'],
                    avg_call_dur,
                    agg_obj['time_per_call'],
                    agg_obj['doubt_to_call_time'],
                    agg_obj['num_doubts'],
                    agg_obj['num_sheets_attempted'],
                    agg_obj['num_items_attempted'],
                    student_url
                ])
    final_request_teacher = {
            "value_input_option": "USER_ENTERED",
            "data": [{
                    "range": "TeacherSheet!A1:Q"+str(len(output_agg)+1),
                    "values": output_agg,
                }]
            }
    request = service.spreadsheets().values().batchUpdate(spreadsheetId=teacher_spreadsheet_id, body=final_request_teacher)
    response = request.execute()
    print('Response', response)

if __name__ == '__main__':
    main()
