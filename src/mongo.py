import pymongo
import time
import os

COLLECTION = 'ERROR'
class Mongo:
    def __init__(self, database_name = 'DigitalShelfPlatform'):
        self.conn_str = os.getenv('CONN_STR')
        self.database_name = database_name
        self.client = pymongo.MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.database_name]

    
    def current_milli_time(self):
        return str(round(time.time() * 1000))

    def addErrorDocument(self, crawl_folder, document, kpi = ''):
        upload_collection = COLLECTION
        if crawl_folder.lower() == 'all':
            upload_collection = COLLECTION + kpi

        document['crawl_folder'] = crawl_folder
        document['kpi'] = kpi
        document['_id'] = document['file_name'] + self.current_milli_time()
        self.db[upload_collection].insert_one(document)

    def getDocumentsForRetry(self, query, kpi = ''):
        splits = query.split('_')
        crawl_folder = splits[1]
        filt = {}
        if len(splits) > 2 and splits[2] != 'all':
            filt['device'] = splits[2]
        filt['crawl_folder'] = crawl_folder
        filt['kpi'] = kpi
        crawl_urls = self.getDocumentsFromCollection(COLLECTION, filt)
        return crawl_urls

    def getDocumentsFromCollection(self, collection, filt = {}):
        result = list(self.db[collection].find(filt))
        self.db[collection].delete_many(filt)
        return result
   
    def deleteAllDocuments(self, collection, filt = {}):
        result = self.db[collection].delete_many(filt)

    def popDocument(self, collection):
        collection = self.db[collection]
        doc = collection.find_one()
        result =  collection.delete_many({'_id': doc['_id']})
        return doc

    def printUrls(self, collection, filt = {}):
        result = self.db[collection].distinct("page_url", filt)
        for res in result:
            print(res)
        return result