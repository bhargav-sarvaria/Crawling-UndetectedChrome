from mongo import Mongo
import threading
import time
import queue
from driver import Driver
import json
from google.cloud import storage
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime
from lxml import html

mongo = Mongo()
COLUMN_ORDER = ["product_id","country","retailer","department","category","page""device","page_url","brand","product_name","sku","position","product_page_url","listing_label","reviews","ratings","date"]


class Crawler:
    def __init__(self, thread_limit = 4):
        self.driver = Driver()
        self.consumerRunning = False
        self.queueMap = {}
        self.THREAD_COUNT_ALLOWED = thread_limit
        self.RUNNING_THREADS = []
        self.storage_client = storage.Client.from_service_account_json('config/dsp_retail_scan_cred.json')
        self.bucket = self.storage_client.get_bucket('dsp-retail-scan')

    def addConfig(self, page_config):
        if 'date' not in page_config:
            page_config['date'] = datetime.today().strftime('%Y-%m-%d')
        retailer = page_config['retailer']
        if retailer in self.queueMap.keys():
            self.queueMap[retailer].put(page_config)
        else:
            qu = queue.Queue()
            qu.put(page_config)
            self.queueMap[retailer] = qu
        if not self.consumerRunning:
            self.consumerRunning = True
            t = threading.Thread(target = self.runConfigConsumer)
            t.start()
    
    def queueNotEmpty(self):
        for retailer, retailer_q in self.queueMap.items():
            if not retailer_q.empty():
                return True
        return False

    def runConfigConsumer(self):
        self.consumerRunning = True
        while self.queueNotEmpty():
            if  len(self.RUNNING_THREADS) < self.THREAD_COUNT_ALLOWED:
                confs = []
                for key, qu in self.queueMap.items():
                    if qu.empty():
                        continue
                    confs.append(qu.get())
                time.sleep(0.1)
                thread_name = str(round(time.time() * 100))
                self.RUNNING_THREADS.append(thread_name)
                t = threading.Thread(target = self.processConfig, name=thread_name, args=(confs,thread_name,))
                t.start()
            else:
                time.sleep(5)

    def crawlUrlsFromConfigPath(self, crawl_folder):
        if 'Retry' in crawl_folder:
            self.crawl_folder = crawl_folder.split('_')[1]
            crawl_urls = mongo.getDocumentsForRetry(crawl_folder.split('_')[1])

            for idx, page_config in enumerate(crawl_urls):
                page_config['index'] = str(idx)
                page_config['url_count'] = str(len(crawl_urls))
                self.addConfig(page_config)
        else:
            self.crawl_folder = crawl_folder
            crawl_path = './page_urls/' + crawl_folder + '/'
            crawl_pages = [crawl_path + f for f in listdir(crawl_path) if isfile(join(crawl_path, f))] 
            for crawl_page in crawl_pages:
                if not os.path.isfile(crawl_page):
                    return False
                if '.json' not in crawl_page:
                    continue
                crawl_urls = json.load(open(crawl_page,'r'))

                for idx, page_config in enumerate(crawl_urls):
                    page_config['index'] = str(idx)
                    page_config['url_count'] = str(len(crawl_urls))
                    self.addConfig(page_config)

    def crawlUrlsFromConfig(self, crawl_pages=None):
        if not os.path.isfile(crawl_pages):
            return False
        crawl_urls = json.load(open(crawl_pages,'r'))
        for page_config in crawl_urls:
            self.addConfig(page_config)

    def retryUrlsFromMongo(self):
        page_configs = mongo.getDocuments(self.crawl_folder)
        for idx, page_config in enumerate(page_configs):
            page_config['index'] = str(idx)
            page_config['url_count'] = str(len(page_configs))
            self.addConfig(page_config)

    def processConfig(self, page_configs,thread_name):
        use_proxy = False
        if 'Proxy' in self.crawl_folder:
            use_proxy = True
        for device in ['Desktop', 'Mobile']:
            d = self.driver.get_driver(use_proxy=use_proxy, device=device)
            for page_config in page_configs:
                page_config['device'] = device
                try:
                    if d == None:
                        d = self.driver.get_driver(use_proxy=use_proxy, device=device)
                    try:
                        d.get(page_config['page_url'])
                    except  TimeoutException as ex:
                        x=1         

                    # Wait for lazy loading
                    self.retailerWait(page_config['retailer'])
                    # self.translateToEnglish(d)
                    d.execute_script("window.scrollTo(0,document.body.scrollHeight);")
                    
                    source = BeautifulSoup(d.page_source, 'html.parser')
                    
                    threading.Thread(target = self.parsePage, args=(source,page_config,device,)).start()      
                    # self.parsePage(source,page_config, device)

                except Exception as e:
                    page_config['message'] = 'processConfig exception'
                    mongo.addErrorDocument(self.crawl_folder, page_config)
                    print('**ERROR**' + page_config['page_url'] + ' ' + '0')    
            try:
                d.close()
                d.quit()
            except:
                x = 1
        self.RUNNING_THREADS.remove(thread_name)
        if len(self.RUNNING_THREADS) == 0 and not self.queueNotEmpty():
            self.consumerRunning = False
    
    def parsePage(self, source, page_config, device):
        try:
            parser = json.load(open(page_config['parsing_config'],'r'))
            products = self.fetchProductsinPage(source, parser['fetch_products']['selectors'])
            if len(products) == 0:
                page_config['message'] = 'No products'
                mongo.addErrorDocument(self.crawl_folder, page_config)
                print('**ERROR**' + page_config['page_url'] + ' ' + '0')
                return
            products_data = self.getProductsData(products, parser['fetch_product'], page_config)
            if len(products_data) == 0:
                page_config['message'] = 'No product details'
                mongo.addErrorDocument(self.crawl_folder, page_config)
                print('**ERROR**' + page_config['page_url'] + ' ' + '0')
                return
            df = pd.DataFrame(products_data)
            df = df.reindex(columns=COLUMN_ORDER)
            filname = page_config['file_name'] + '_' + device + '_' + page_config['date'] + '.csv'
            df.to_csv(filname, index=False)
            gcloud_filename = page_config['gcloud_path'] + page_config['date'] + '/' + filname;
            self.bucket.blob(gcloud_filename).upload_from_filename(filname)
            if os.path.exists(filname):
                os.remove(filname)
                print('completed: ' + page_config['retailer'] + ' ' + page_config['index'] + '/' + page_config['url_count'] + ' ' + str(len(products)) )
        except Exception as e:
            page_config['message'] = 'parsePage exception'
            mongo.addErrorDocument(self.crawl_folder, page_config)
            print('**ERROR**' + page_config['page_url'] + ' ' + '0')

    def fetchProductsinPage(self, element, selectors):
        elements = []
        for selector in selectors:
            if selector['type'] == 'classname':
                elements = element.find_all(class_= selector['value'])
            elif selector['type'] == 'classname_attribute_condition':
                elements = element.find_all(class_= selector['value'])
                final_elements = []
                for el in elements:
                    try:
                        if el.get(selector['attribute_key']):
                            if el.get(selector['attribute_key']) == selector['attribute_value']:
                                final_elements.append(el)
                    except:
                        continue
            elif selector['type'] == 'tagname':
                elements = element.find_all(selector['value'])    
            elif selector['type'] == 'tagname_attribute':
                elements = element.find_all(selector['value'])
                final_elements = []
                for el in elements:
                    if el.get(selector['attribute_key']) == selector['attribute_value']:
                        final_elements.append(el)
                elements = final_elements
            elif selector['type'] == 'css_selector':
                elements = element.select(selector['value'])
            elif selector['type'] == 'classname_xpath':
                    el = element.find( class_= selector['classname'])
                    value = html.fromstring(el.prettify()).xpath(selector['xpath'])[0].text_content().strip()

            # Keep this at bottom, deleted unwanted tags before coming to actual tag
            if selector['type'] == 'delete_classname':
                classnames = selector['delete_value'].split(', ')
                for classname in classnames:
                    while element.find(class_= classname):
                        element.find(class_= classname).decompose()
                elements = element.find_all(class_= selector['value'])
            if len(elements) > 0:
                    return elements
        return elements
    
    def fetchTextFromSelectors(self, element, selectors, page_config = {}):
        for selector in selectors:
            value = ''
            try:
                if selector['type'] == 'attribute':
                    value = element.get(selector['value'])
                elif selector['type'] == 'config_value':
                    value = page_config[selector['value']]
                elif selector['type'] == 'classname':
                    value = element.find( class_= selector['value']).getText()
                elif selector['type'] == 'classnames':
                    value = ''
                    for selection in selector['values']:
                        if element.find( class_= selection):
                            value = ' '.join([value, element.find( class_= selection).getText()])
                elif selector['type'] == 'classname_attribute':
                    value = element.find(class_= selector['value']).get(selector['selector_attribute'])
                elif selector['type'] == 'classname_attribute_condition':
                    elements = element.find_all(class_= selector['value'])
                    for el in elements:
                        try:
                            if el.get(selector['attribute_key']):
                                if el.get(selector['attribute_key']) == selector['attribute_value']:
                                    value = el.getText()
                                    if value:
                                        break
                        except:
                            continue
                elif selector['type'] == 'classname_attribute_objectvalue':
                    value = json.loads(element.find(class_= selector['value']).get(selector['selector_attribute']))[selector['object_key']]
                elif selector['type'] == 'tagname':
                    value = element.find(selector['value']).getText()
                elif selector['type'] == 'tagname_attribute':
                    value = element.find(selector['value']).get(selector['selector_attribute'])
                elif selector['type'] == 'tagname_attribute_condition':
                    elements = element.find_all(selector['value'])
                    for el in elements:
                        try:
                            if el.get(selector['attribute_key']):
                                if el.get(selector['attribute_key']) == selector['attribute_value']:
                                    value = el.getText()
                                    if value:
                                        break
                        except:
                            continue
                elif selector['type'] == 'xpath_attribute':
                    value = html.fromstring(element.prettify()).xpath(selector['value'])[0].get(selector['selector_attribute'])
                elif selector['type'] == 'attribute_objectvalue':
                    value = json.loads(element.get(selector['value']))[selector['object_key']]
                elif selector['type'] == 'classname_xpath':
                    el = element.find( class_= selector['classname'])
                    value = html.fromstring(el.prettify()).xpath(selector['xpath'])[0].text_content().strip()
                elif selector['type'] == 'classname_xpath_attribute':
                    el = element.find( class_= selector['classname'])
                    value = html.fromstring(el.prettify()).xpath(selector['xpath'])[0].get(selector['selector_attribute'])
                elif selector['type'] == 'classname_split':
                    value = element.find( class_= selector['classname']).text.replace(u'\xa0', u' ')
                    value = value.split(selector['splitter'])[selector['split']]
                elif selector['type'] == 'attribute_split':
                    value = element.get(selector['selector_attribute'])
                    value = value.split(selector['splitter'])[selector['split']]
                elif selector['type'] == 'classname_attribute_split':
                    value = element.find( class_= selector['classname']).get(selector['selector_attribute'])
                    value = value.split(selector['splitter'])[selector['split']]
                elif selector['type'] == 'classname_delete_classname':
                    el = element.find(class_= selector['value'])
                    el.find(class_= selector['delete']).decompose()
                    value = el.getText()
                elif selector['type'] == 'delete_classname':
                    element.find(class_= selector['value']).decompose()
                
            except Exception as e:
                value = ''
                            
            if value is not None and value!= '':
                # try:
                    # tr = translator.translate(value.strip(),src='fr', dest='en').text
                # except Exception as e:
                return value.strip()
            else:
                value = ''
                continue
        return ''

    def getProductsData(self, products, product_attrs, page_config):
        products_data = []
        position_counter = 1
        try:
            for product in products:
                product_data = {}
                for product_attr in product_attrs:
                    field = product_attr['field']
                    if field != 'position': 
                        product_data[field] = self.fetchTextFromSelectors(product, product_attr['selectors'], page_config)
                    elif field == 'position':
                        if product_data['product_name'] == '' or product_data['product_name'] is None:
                            continue
                        product_data[field] = position_counter
                        position_counter += 1
                if product_data['product_name'] == '' or product_data['product_name'] is None:
                    continue
                products_data.append(product_data)
        except Exception as e:
            return []
        return products_data

    def retailerWait(self, retailer):
        if retailer in ['Sephora', 'Nykaa']:
            time.sleep(1.5)
        elif retailer in ['Myer']:
            time.sleep(2)
        if retailer in ['Harrods', 'Selfridges_UK']:
            time.sleep(2.5)

    def translateToEnglish(self, d):
        if 'en' not in d.find_element(By.TAG_NAME, 'html').get_attribute('lang') and d.find_element(By.TAG_NAME, 'html').get_attribute('lang'):
            execu = '''
            var scr = document.createElement('div');
            scr.className = "rightClick";
            document.body.appendChild(scr);
            '''
            d.execute_script(execu)
            ActionChains(d).context_click(d.find_element(By.CLASS_NAME, 'rightClick')).perform()
            time.sleep(0.5)
            import pyautogui
            for i in range(8):
                pyautogui.press('down')
            pyautogui.press('enter')
            d.execute_script("window.scrollTo(0, 0);")
            for i in range(0,10):
                time.sleep(0.5)
                d.execute_script("window.scrollTo(0,"+str(i)+"*(document.body.scrollHeight/10));")

    