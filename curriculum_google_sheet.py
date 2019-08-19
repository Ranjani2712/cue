import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from update_from_cuedb import *
from pprint import pprint
import numpy as np


fl_nm_pickle = './google_sheets/token.pickle'
fl_nm_program_sheet_map = './google_sheets/program_sheet_map.json'
fl_nm_chapter_sheet_map = './google_sheets/chapter_sheet_map.json'
inteldb = IntelDB('prod')

def initialize_creds():
    creds = None
    if os.path.exists(fl_nm_pickle):
        with open(fl_nm_pickle, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(fl_nm_pickle, 'wb') as token:
            pickle.dump(creds, token)
    if(creds is not None):
        return creds
    else:
        raise Exception('No Credentials Formed')


def update_chapter_data(chapter, spreadsheet_id, sheet_id, sheet_nm):
    chapter_code = chapter.get('_id')
    index = 0
    data = []
    data.append(['Type', 'Program_Code', 'Chapter_Code', 'Chapter_Version', 'Position', 'Activity Reference', 'Item Reference', '#Widgets', 'Median Time (mins)', 'Accuracy(%)', '#Data Points'])
    node_cursor = inteldb.node_collection.collection.find({'chapter_code':chapter_code})
    activity_id_list, node_dict = [], {}
    for node in node_cursor:
        node_dict[node.get('_id')] = node
        activity_id_list.append(node.get('activity_id'))
    activities = inteldb.activity_collection.collection.find({'_id':{'$in':activity_id_list}})
    for idx, activity in enumerate(activities):
        total_widgets = 0
        print("activity #", idx, " : ", activity.get('reference'))
        if(activity.get('distribution_time')):
            num_data_pts = len(activity.get('distribution_time', {}))
            all_times = list(activity.get('distribution_time', {}).values())
            median_time = np.percentile(all_times, 50)/60.0
            all_scores = list(activity.get('distribution_score', {}).values())
            median_score = np.percentile(all_scores, 50)
            nodes_linked = activity.get('nodes_linked', [])
            for node_id in nodes_linked:
                node = node_dict.get(node_id)
                if(node is None or node.get('chapter_code', 'NA').split('.')[-1] == "INTROD"):
                    continue
                item_ref_list = [item_ref for item_ref in activity.get('data').get('items')]
                items_cursor_list = inteldb.item_collection.collection.find({'_id':{'$in':item_ref_list}})
                item_dict = {}
                for itm in items_cursor_list:
                    item_dict[itm.get('_id')] = itm
                    total_widgets += len(itm.get('definition').get('widgets'))
                index += 1
                data.append([
                        'Worksheet',
                        str(node.get('program_code', 'NA')),
                        str(node.get('chapter_code', 'NA')),
                        str(node.get('chapter_version', 0)),
                        str(node.get('position_in_chapter', 0)),
                        activity.get('reference').replace(',', '-'),
                        'NA',
                        str(total_widgets),
                        str(median_time),
                        str(median_score/activity.get('max_score')*100),
                        str(num_data_pts)])
                for item_ref, item in item_dict.items():
                    # item = inteldb.item_collection.collection.find_one({'_id':item_refs})
                    #Get Item Responses and append to below data
                    if(item is not None):
                        widgets_in_item = len(item.get('definition').get('widgets'))
                        max_score_item = item.get('max_score')
                        if(max_score_item is not None and max_score_item!=0 and len(item.get('distribution_score'))>1):
                            score_list_item = list(item.get('distribution_score').values())
                            time_list_item = list(item.get('distribution_time').values())
                            median_score_item = np.mean(score_list_item)
                            median_time_item = np.percentile(time_list_item, 50)/60.0
                        elif(max_score_item is not None and max_score_item!=0 and len(item.get('distribution_score'))==1):
                            score_list_item = list(item.get('distribution_score').values())
                            median_score_item = score_list_item[0]
                            median_time_item = list(item.get('distribution_time').values())[0]/60.0
                        else:
                            continue
                        index += 1
                        data.append([
                            'Item',
                            str(node.get('program_code', 'NA')),
                            str(node.get('chapter_code', 'NA')),
                            str(node.get('chapter_version', 0)),
                            str(node.get('position_in_chapter', 0)),
                            activity.get('reference').replace(',', '-'),
                            item_ref.replace(',', '-'),
                            str(widgets_in_item),
                            str(median_time_item),
                            str((float(median_score_item)/max_score_item)*100.0),
                            str(len(score_list_item))])
    print('DONE idx', index, sheet_nm +'!A1:K' + str(index+3))
    if(activities is not None and index > 0):
        val_range_obj = {
                "range": sheet_nm +"!A1:K" + str(index+3) ,
                "values": data
            }
        return val_range_obj
    else:
        return None


def update_program_sheet(service, program_code, spreadsheet_id):
    chapters = inteldb.chapter_collection.collection.find({'program_code':program_code}).sort('position')
    val_range_list = []
    with open(fl_nm_chapter_sheet_map, "r") as fp:
        chapter_sheet_map = json.load(fp)
    for idx, chapter in enumerate(chapters):
        sheet_id = chapter_sheet_map.get(chapter.get('_id'))
        if(sheet_id is None):
            new_sheet = {
                "requests": [{
                    "addSheet": {
                        "properties":{
                            "title":chapter.get('name'),
                            }
                        }
                    }]
                }
            request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=new_sheet)
            response = request.execute()
            # pprint(response)
            replies = response.get('replies')
            sheet_id = replies[0].get('addSheet').get('properties').get('sheetId')
            chapter_sheet_map[chapter.get('_id')] = sheet_id
        new_chapter_data = update_chapter_data(chapter, spreadsheet_id, sheet_id, chapter.get('name'))
        if(new_chapter_data is not None):
            val_range_list.append(new_chapter_data)
    with open(fl_nm_chapter_sheet_map, "w") as fp:
        json.dump(chapter_sheet_map, fp)
    if(len(val_range_list)>0):
        pprint(val_range_list[0])
    final_request = {
        "value_input_option": "USER_ENTERED",
        "data": val_range_list,
    }
    request = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=final_request)
    response = request.execute()
    print('Response', response)



def main():
    program_cursor = inteldb.program_collection.collection.find()
    creds = initialize_creds()
    service = build('sheets', 'v4', credentials=creds)
    with open(fl_nm_program_sheet_map, "r") as fp:
        program_ws_map = json.load(fp)
    for program in program_cursor:
        if(not program.get('_id').startswith('DEMO') and not program.get('_id')=='CBSE.G7'):
            spreadsheet_id = program_ws_map.get(program.get('_id'))
            if(spreadsheet_id is None):
                new_sheet = {
                        "properties": {
                            "title": program.get('_id')
                            },
                        "sheets":{
                            "properties": {
                                "title":'Overview',
                            }
                        }
                    }
                request = service.spreadsheets().create(body=new_sheet)
                try:
                    response = request.execute()
                except googleapiclient.errors.HttpError as e:
                    pprint(e)
                spreadsheet_id = response.get('spreadsheetId')
                program_ws_map[program.get('_id')] = spreadsheet_id
            update_program_sheet(service, program.get('_id'), spreadsheet_id)
    with open(fl_nm_program_sheet_map, "w") as fp:
        json.dump(program_ws_map, fp)


if __name__ == '__main__':
    main()

