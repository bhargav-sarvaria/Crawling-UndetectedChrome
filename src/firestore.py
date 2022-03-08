from google.cloud import firestore
import time

class Firestore:
    def __init__(self, thread_limit = 4):
        self.client = firestore.Client.from_service_account_json('./config/dsp_retail_scan_cred.json')
    
    def addDocument(self, collection, document):
        if not collection:
            return
        doc_ref = self.client.collection(collection).document(str(round(time.time() * 100)))
        doc_ref.set(document)
        return
    
    def printDocuments(self, collection):
        docs = self.client.collection(collection).stream()
        for doc in docs:
            print(doc.to_dict()['page_url'])

    def deleteDocuments(self, collection):
        docs = self.client.collection(collection).stream()
        for doc in docs:
            self.client.collection(collection).document(doc.id).delete()

    def popDocument(self, collection):
        collection = self.client.collection(collection)
        query = collection.limit(1).get()
        if len(query):
            # print(query)
            doc = query[0]
            collection.document(doc.id).delete()
            return doc.to_dict()
    
    def getDocuments(self, collection):
        collection = self.client.collection(collection)
        docs = collection.stream()
        crawl_pages = []
        for doc in docs:
            crawl_pages.append(doc.to_dict())
            collection.document(doc.id).delete()
        return crawl_pages