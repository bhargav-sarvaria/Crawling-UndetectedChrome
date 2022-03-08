import pymongo

class Mongo:
    def __init__(self, database_name = 'DigitalShelfPlatform'):
        self.conn_str = "mongodb+srv://digitalshelfplatform:Ascent123A@digitalshelfplatform.wpkbc.mongodb.net/digitalShelfPlatform?retryWrites=true&w=majority"
        self.database_name = database_name
        self.client = pymongo.MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.database_name]

    def addDocument(self, collection, document):
        result = self.db[collection].insert_one(document)

    def getDocuments(self, collection):
        result = list(self.db[collection].find())
        return result
   
    def deleteAllDocuments(self, collection):
        result = self.db[collection].delete_many({})

    def popDocument(self, collection):
        collection = self.db[collection]
        doc = collection.find_one()
        result =  collection.delete_many({'_id': doc['_id']})
        return doc
    

mongo = Mongo()
# mongo.addDocument('India', {'test': 1})
# mongo.addDocument('India', {'test': 2})
mongo.deleteAllDocuments('India')
# mongo.deleteAllDocuments('United States')
# mongo.deleteAllDocuments('United Kingdom')
# mongo.deleteAllDocuments('EU')
# mongo.deleteAllDocuments('Douglas')
# print(mongo.popDocument('India'))
# mongo.printDocuments('India')
# print(mongo.getDocuments('India'))
# mongo.deleteAllDocuments('India')


