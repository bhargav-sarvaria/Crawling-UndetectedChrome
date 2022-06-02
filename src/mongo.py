import pymongo
import time
COLLECTION = 'ERROR'
class Mongo:
    def __init__(self, database_name = 'DigitalShelfPlatform'):
        self.conn_str = "mongodb+srv://digitalshelfplatform:Ascent123A@digitalshelfplatform.wpkbc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
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
        crawl_folder = query.split('_')[1]
        device = query.split('_')[2]
        if crawl_folder.lower() == 'all':
            crawl_urls = self.getAllDocumentsForRetry(device, kpi)
        else:
            if device.lower() == 'all':
                filt = {"crawl_folder": crawl_folder,  "kpi": kpi}
            else:
                filt = {"crawl_folder": crawl_folder, "kpi": kpi, "device": device }
            crawl_urls = self.getDocumentsFromCollection(COLLECTION, filt)
        return crawl_urls
    
    def getAllDocumentsForRetry(self, device, kpi = ''):
        filt = {"device": device, "kpi": kpi}
        failed_urls = []
        for collection in self.db.list_collection_names():
            if 'all' in collection.lower():
                continue
            result = list(self.db[collection].find(filt))
            failed_urls.extend(result)
            self.db[collection].delete_many(filt)
        return failed_urls

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
        result = list(self.db[collection].find(filt, {'_id': 0, 'page_url': 1}))
        # result = self.db[collection].distinct("page_url", {"crawl_folder": "United States"})
        for res in result:
            print(res)
        return result
    

# mongo = Mongo()
# mongo.deleteAllDocuments('ERROR', {'date': {'$ne': '2022-06-02' }})
# mongo.printUrls('ERROR')
# mongo.addDocument('India', {'test': 1})
# mongo.addDocument('India', {'test': 2})
# mongo.deleteAllDocuments('ERROR', {'date': {'$ne': '2022-06-02' }})
# mongo.deleteAllDocuments('Australia')
# mongo.deleteAllDocuments('India')
# mongo.deleteAllDocuments('United States')
# mongo.deleteAllDocuments('United Kingdom')
# mongo.deleteAllDocuments('EU')
# mongo.deleteAllDocuments('Douglas')
# print(mongo.popDocument('India'))
# mongo.printUrls('ERROR')
# print(mongo.getDocuments('India'))
# mongo.deleteAllDocuments('India')