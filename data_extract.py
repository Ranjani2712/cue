import json

from learnosity_sdk.request import DataApi

security = {
    'consumer_key': 'ds8NJIkkTdWZkYZq',
    'domain': 'localhost'
}
# WARNING: The consumer secret should not be committed to source control.
consumer_secret = 'WvsYiUZIr2irK8Qx0YLJr54KjbtT9TJfPCD7gnQX'

endpoint = 'https://data.learnosity.com/v1/sessions/responses'
endpoint_items = 'https://data.learnosity.com/v1/itembank/items'
endpoint_activity = 'https://data.learnosity.com/v1/itembank/activities'
data_request_session = {
		#"include":["activity_template_ids"]
		"activity_id": ["411812ec-c545-11e8-9983-c4b301c785cf",
						"4110f8a2-c545-11e8-81ff-c4b301c785cf",
						"410a6bfe-c545-11e8-856b-c4b301c785cf",
						"40c80a98-c545-11e8-8fe3-c4b301c785cf",
						"40c16eb8-c545-11e8-85a5-c4b301c785cf",
						"40ba5b64-c545-11e8-985a-c4b301c785cf",
						"40b34d68-c545-11e8-922d-c4b301c785cf",
						"40ab9ed0-c545-11e8-96c7-c4b301c785cf",
						"40a4e4f0-c545-11e8-b17f-c4b301c785cf",
						"409e192c-c545-11e8-8c43-c4b301c785cf",
						"409780ba-c545-11e8-aedc-c4b301c785cf",
						"4090ce34-c545-11e8-b61d-c4b301c785cf",
						"408a403e-c545-11e8-a754-c4b301c785cf",
						"4083a0c6-c545-11e8-a151-c4b301c785cf",
						"407cc2a4-c545-11e8-a1d8-c4b301c785cf",
						"4076105a-c545-11e8-bd54-c4b301c785cf",
						"406f7298-c545-11e8-a79a-c4b301c785cf",
						"4068b1ec-c545-11e8-9a33-c4b301c785cf",
						"2b147a2e-d76a-11e8-b708-c4b301c785cf",
						"2b0de39c-d76a-11e8-8cb1-c4b301c785cf",
						"2b07225c-d76a-11e8-adf5-c4b301c785cf",
						"2b008078-d76a-11e8-b339-c4b301c785cf",
						"2af7c5ac-d76a-11e8-adac-c4b301c785cf",
						"2aee73ec-d76a-11e8-a273-c4b301c785cf",
						"2ae7cf10-d76a-11e8-938e-c4b301c785cf",
						"2ae12e26-d76a-11e8-8a17-c4b301c785cf",
						"2ada97e6-d76a-11e8-a210-c4b301c785cf",
						"2ad3e630-d76a-11e8-bb4f-c4b301c785cf",
						"2acc2e74-d76a-11e8-9a0c-c4b301c785cf",
						"2ac4fda8-d76a-11e8-ab5c-c4b301c785cf",
						"2abe5f9a-d76a-11e8-a205-c4b301c785cf",
						"2ab7af1c-d76a-11e8-889c-c4b301c785cf",
						"2ab0b408-d76a-11e8-aee3-c4b301c785cf",
						"2aa7dc70-d76a-11e8-a77b-c4b301c785cf",
						"2aa0daa6-d76a-11e8-bcda-c4b301c785cf",
						"2a973b2c-d76a-11e8-9942-c4b301c785cf"
						],
		#"activity_id": ['2a973b2c-d76a-11e8-9942-c4b301c785cf']
		"status": [	
		"Completed"
		],
		"sort": "asc",
		"mintime": "2018-06-01",
		"limit": 50
}
data_request_activity = {
	"sort": "asc",
	#"mintime": "2018-11-07",
	"include":{
		"activities":["dt_created", "dt_updated", "last_updated_by"]
	},
	#"references": ["commerce-recap-profit-loss-discount-tax.w01"]	
}

data_request_items = {
	"sort": "asc",
	#"mintime": "2018-11-07",
	"include":{
		"items":["dt_created", "dt_updated", "last_updated_by"]
	},
	#"references": ["Sherlock_Numbers_S048a", "Lines And Angles - Basics Of Lines And Angles - 033"]	
}



action = 'get'

client = DataApi()
res = client.results_iter(endpoint_activity, security, consumer_secret, data_request_activity, action)
item_list = dict()
for idx, item in enumerate(res):
	item_list[str(idx)] = item
	print("API Hit No.", idx)
with open('../Data/activity_data_dump.json', "w") as fp:
	json.dump(item_list, fp)

res = client.results_iter(endpoint_items, security, consumer_secret, data_request_items, action)
item_list = dict()
for idx, item in enumerate(res):
	item_list[str(idx)] = item
with open('../Data/items_data_dump.json', "w") as fp:
	json.dump(item_list, fp)



	#print(idx, item['reference'], item["dt_created"], item["dt_updated"])
	#print((idx+1), "activity_id", item['activity_id'], item['reference'])
# make a single request for the first page of results
# returns a requests.Response object
'''
res = client.results_iter(endpoint, security, consumer_secret, data_request_session, action)
# print the length of the items list (for the first page)
#print(len(res.json()))
with open("./Data/dump_test.json", "w") as fp:
	fp.write('{"final_data":[ \n')
#for idx, result in enumerate(res):
toggle=True
idx=0
result=next(res)
while(toggle):
#item_list=[]
#for idx, result in enumerate(res):
	idx+=1
	print((idx+1), "activity_id", result['activity_id'])
#	item_list.append(result)
#item_dict={"final_data":item_list}
#with open("./Data/dump_test.json", "w") as fp:
#	json.dump(item_dict, fp)

	
	with open("./Data/dump_test.json", "a") as fp:
		try:
			json.dump(result, fp)
			result=next(res)
			fp.write(',\n')
		except StopIteration:
			fp.write("\n ] \n }")
			toggle=False

			#break
	#print(result)
#with open("./Data/dump_test.json", "a") as fp:
#	fp.write("] \n }")'''

#print(res.json()["meta"])

# iterate over all results
# this returns an iterator of results, abstracting away the paging

'''
items=[]
activity_list = []
for idx, item in enumerate(client.results_iter(endpoint_activity, security, consumer_secret, data_request_activity, action)):
    # prints each item in the result
	#print(idx, json.dumps(item))
	items.append(item)
	activity_list.append(item["activity_id"])
# request all results as a list
# using `list` we can easily download all the results into a single list
#data_request_session["activity_id"].append('IMO_G7_MOCK_5')
#items = list(client.results_iter(endpoint, security, consumer_secret, data_request_session, action))
# print the length of the items list (will print the total number of items)
print(len(items))
print("activity_ids", activity_list)
#print(items)
# iterate over each page of results
# this can be useful if the result set is too big to practically fit in memory all at once
for result in client.request_iter(endpoint, security, consumer_secret, data_request, action):
    # print the length of each page of the items list (will print a line for each page in the results)
	print(len(result['data']))
'''