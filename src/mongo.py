import pymongo
import time

class Mongo:
    def __init__(self, database_name = 'DigitalShelfPlatform'):
        self.conn_str = "mongodb+srv://digitalshelfplatform:Ascent123A@digitalshelfplatform.wpkbc.mongodb.net/digitalShelfPlatform?retryWrites=true&w=majority"
        self.database_name = database_name
        self.client = pymongo.MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.database_name]

    
    def current_milli_time(self):
        return str(round(time.time() * 1000))

    def addDocument(self, collection, document):
        document['_id'] = document['file_name'] + self.current_milli_time()
        result = self.db[collection].insert_one(document)

    def addErrorDocument(self, collection, document):
        # result = list(self.db[collection].find({
        #     "page_url": document["page_url"], 
        #     "date": document["date"]
        # }))
        # if not len(result):
        document['_id'] = document['file_name'] + self.current_milli_time()
        result = self.db[collection].insert_one(document)

    def getDocuments(self, collection):
        result = list(self.db[collection].find())
        return result

    
    def getAllDocumentsForRetry(self, device):
        filt = {"device": device}
        failed_urls = []
        for collection in self.db.list_collection_names():
            filt = {"device": device}
            result = list(self.db[collection].find(filt))
            failed_urls.extend(result)
            self.db[collection].delete_many(filt)
        return failed_urls

    def getDocumentsForRetry(self, collection, device):
        filt = {"device": device}
        result = list(self.db[collection].find(filt))
        self.db[collection].delete_many(filt)
        return result
   
    def deleteAllDocuments(self, collection):
        result = self.db[collection].delete_many({"date": "2022-04-21"})

    def popDocument(self, collection):
        collection = self.db[collection]
        doc = collection.find_one()
        result =  collection.delete_many({'_id': doc['_id']})
        return doc

    def printUrls(self, collection):
        result = list(self.db[collection].find({}, {'_id': 0, 'page_url': 1}))
        for res in result:
            print(res)
        return result
    

mongo = Mongo()
# mongo.printUrls('United Kingdom')
# mongo.addDocument('India', {'test': 1})
# mongo.addDocument('India', {'test': 2})
mongo.printUrls('All')
mongo.printUrls('Australia')
mongo.printUrls('India')
mongo.printUrls('United States')
mongo.printUrls('United Kingdom')
mongo.printUrls('EU')
mongo.printUrls('Douglas')
# print(mongo.popDocument('India'))
# mongo.printDocuments('India')
# print(mongo.getDocuments('India'))
# mongo.deleteAllDocuments('India')