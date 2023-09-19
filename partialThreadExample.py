import json
from datetime import datetime, timedelta
import pymongo
import pprint
import sys

try:

  # the following accesses the Mongodb Atlas server 
  userName = "kspar1dashtwo"
  passWord = "PASSWORD_HERE"
  mongoURL = f"mongodb+srv://{userName}:{passWord}@cluster0.00wbctk.mongodb.net/?retryWrites=true&w=majority"
  print("Database URL: " + mongoURL)
  client = pymongo.MongoClient(mongoURL)
  
except pymongo.errors.ConfigurationError:
  print("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
  sys.exit(1)

db = client.dashDatabase

# use a collection named "processesY"
processesCollection = db["rajuProcesses"]

# T E S T   D A T A 
testProcessDocuments = [{
   "timeStamp": datetime.utcnow(),
"processes" : [
   { 
   "processCount": 1,
   "processInspectionType": "plastic",
   "title": "Plastic Inspection",
   "typeNode": "processNode",
   "procDate": datetime.utcnow(),
      "data": {
         "inputRate": 3600,  
         "serviceRate": 3000
      }
   },
   { 
   "processCount": 2,
   "processInspectionType": "metal",
   "title": "Metal Inspection",
   "typeNode": "processNode",
   "procDate": datetime.utcnow(),
      "data": {
         "inputRate": 3200,  
         "serviceRate": 2800
      }
   }
   ]
},{
   "timeStamp": datetime.utcnow(),
"processes" : [
   { 
   "processCount": 1,
   "processInspectionType": "plastic",
   "title": "Plastic Inspection",
   "typeNode": "processNode",
   "procDate": datetime.utcnow(),
      "data": {
         "inputRate": 3700,  
         "serviceRate": 2900
      }
   },
   { 
   "processCount": 2,
   "processInspectionType": "metal",
   "title": "Metal Inspection",
   "typeNode": "processNode",
   "procDate": datetime.utcnow(),
      "data": {
         "inputRate": 3300,  
         "serviceRate": 2900
      }
   }
   ]
}]
# drop the collection in case it already exists
# use this during development time only.  
# NOTE: Remove when working with data uploaded from CSV.
try:
  processesCollection.drop()  

except pymongo.errors.OperationFailure:
  print("An authentication error was received. Are your username and password correct in your connection string?")
  sys.exit(1)

# INSERT TEST DOCUMENTS
try: 
  result = processesCollection.insert_many(testProcessDocuments)
except pymongo.errors.OperationFailure:
  print("An authentication error was received. Are you sure your database user is authorized to perform write operations?")
  sys.exit(1)
else:
  inserted_count = len(result.inserted_ids)
  print("I inserted %x documents." %(inserted_count))

  print("\n")

# FIND DOCUMENTS
result = processesCollection.find({}) # get all documents
if result:    
  for process in result:
    pprint.pprint(process) # well-formatted, readable JSON document
    print("-------\n")

    id = process['_id']
    timeStamp =  process['timeStamp']
    formatted_time = timeStamp.strftime('%H:%M:%S')
    print(formatted_time)

    print("Date:" + str(timeStamp.date()))
    #print('Timestamp: ' + process['timeStamp'].date())

    #db.collection.find( { field: { $size: 2 } } );

    processArray = process['processes'] 
    pprint.pprint(processArray)

    ct = 0
    for processDetails in processArray:
      print("\n")
      print(f"ID:{id}, title:{processDetails['title']}, processCount:{str(processDetails['processCount'])}, \
       inputRate:{str(processDetails['data']['inputRate'])}")

      ### NEW CODE that updates existing collection with minVal field (under processes.data)

      inputRate = processDetails['data']['inputRate']
      serviceRate = processDetails['data']['serviceRate'] 
  
      minVal = min(inputRate,serviceRate)
      processesCollection.update_one(
        {"_id":id},
        {"$set" : {"processes." + str(ct) + ".data.newVal" : minVal}}
                                     )
      ct += 1
      
      ### end of new code
else:
  print("No documents found.")

print("\n")

# get a count of documents using aggregate before fetching data
