from driver import Driver
from mongo import Mongo
mongo = Mongo()

import numpy as np
import threading
import time
import queue
import json
from google.cloud import storage
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import sys
from os import listdir
from os.path import isfile, join
from datetime import datetime
from lxml import html
import logging as LOGGING
import psutil
import re
from typing import List

RENDER_WAIT_LIMIT = 3
MEMORY_THRESHOLD = 15
DRIVER_CLEAN_TIME = 600
DRIVER_CLEAN_TIME_WAIT = DRIVER_CLEAN_TIME

LOGGING.basicConfig(
filename='run.log',
filemode='a',
format='[%(asctime)s] %(levelname)s {%(filename)s:%(lineno)d} -  %(message)s',
datefmt='%H:%M:%S',
level=LOGGING.WARN
)

COLUMN_ORDER = ["product_id","country","retailer","department","category", "keyword", "page","device","page_url","brand","product_name","sku","position","product_page_url","listing_label","reviews","ratings","date", "sponsored_flag", "full_page_snapshot"]


class Crawler:
    def __init__(self, thread_limit = 2):
        self.driver = Driver()
        self.consumerRunning = False
        self.queueMap = {}
        self.THREAD_COUNT_ALLOWED = thread_limit
        self.RUNNING_THREADS = []
        self.ACTIVE_DRIVERS = []
        self.storage_client = storage.Client.from_service_account_json('config/dsp_retail_scan_cred.json')
        self.bucket = self.storage_client.get_bucket('dspretailscan')
        self.parser_map = {}

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
            threading.Thread(target = self.runConfigConsumer).start()
            if sys.platform.endswith("linux"):
                threading.Thread(target = self.driverCleaner).start()        
    
    def queueHasItems(self):
        for retailer, retailer_q in self.queueMap.items():
            if not retailer_q.empty():
                return True
        return False

    def runConfigConsumer(self):
        self.consumerRunning = True
        while self.queueHasItems():
            if  len(self.RUNNING_THREADS) < self.THREAD_COUNT_ALLOWED:
                confs = []
                for key, qu in self.queueMap.items():
                    if qu.empty():
                        continue
                    confs.append(qu.get())
                time.sleep(0.1)
                thread_name = str(round(time.time() * 100))
                self.RUNNING_THREADS.append(thread_name)
                t = threading.Thread(target = self.processPLConfig, name=thread_name, args=(confs,thread_name,))
                t.start()
            else:
                time.sleep(5)

    def crawlUrlsFromConfigPath(self, crawl_folder):
        if 'Retry' in crawl_folder:
            RENDER_WAIT_LIMIT = 10
            self.crawl_folder = crawl_folder.split('_')[1]
            crawl_urls = mongo.getDocumentsForRetry(crawl_folder, kpi = 'PL')

            for idx, page_config in enumerate(crawl_urls):
                if page_config['parsing_config'] not in self.parser_map:
                    self.parser_map[page_config['parsing_config']] = json.load(open(page_config['parsing_config'],'r'))
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
                
                # Store the opened config file in local variable
                if len(crawl_urls):
                    self.parser_map[crawl_urls[0]['parsing_config']] = json.load(open(crawl_urls[0]['parsing_config'],'r'))
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
    
    def get_activeDriver(self, d):
        return { "obj": d, "create_time": time.time(), "pids": self.driver_proc(d)}

    def processPLConfig(self, page_configs,thread_name):
        use_proxy = False
        timeout = 10
        devices = ['Desktop', 'Mobile']
        if 'Proxy' in self.crawl_folder:
            use_proxy = True
        if 'device' in page_configs[0]:
            timeout = 20
            devices = [page_configs[0]['device']]
        for device in devices:
            d = self.driver.get_driver(use_proxy=use_proxy, device=device, timeout=timeout)
            active_driver = self.get_activeDriver(d)
            self.ACTIVE_DRIVERS.append(active_driver)
            for page_config in page_configs:
                page_config['device'] = device
                try:
                    try:
                        d.get(page_config['page_url'])
                    except  TimeoutException:
                        pass
                    except Exception as e:
                        LOGGING.error(e)
                        self.driver.quitDriver(d)
                        self.activeDriverRemove(active_driver)
                        d = self.driver.get_driver(use_proxy=use_proxy, device=device,timeout=timeout)
                        active_driver = self.get_activeDriver(d)
                        self.ACTIVE_DRIVERS.append(active_driver)
                        d.get(page_config['page_url'])
                    
                    # Wait for lazy loading
                    self.retailerWait(d, page_config, device)
                    # self.translateToEnglish(d)
                    
                    source = BeautifulSoup(d.page_source, 'html.parser')
                    
                    img_path = './' + str(time.time())+ '.jpg'
                    self.driver.save_screenshot(d, img_path)
                    
                    threading.Thread(target = self.parsePLPage, args=(source,page_config,device,img_path,)).start()
                    # self.parsePLPage(source,page_config, device, img_path)

                except Exception as e:
                    LOGGING.error(e)
                    self.pageError(page_config, 'processPLConfig exception')
            
            self.driver.quitDriver(d)
            self.activeDriverRemove(active_driver)
        
        self.RUNNING_THREADS.remove(thread_name)
        if len(self.RUNNING_THREADS) == 0 and not self.queueHasItems():
            self.consumerRunning = False
    
    def parsePLPage(self, source, page_config, device, img_path):
        try:
            parser = self.parser_map[page_config['parsing_config']]
            products = self.driver.fetchSoupElements(source, parser['fetch_products']['selectors'])
            if len(products) == 0:
                self.pageError(page_config, 'No products', delete=img_path)
                return
            products_data = self.getProductsData(products, parser['fetch_product'], page_config)
            if len(products_data) == 0:
                self.pageError(page_config, 'No product details', delete=img_path)
                return

            
            filename = page_config['file_name'] + '_' + device + '_' + page_config['date'] + '.csv'
            filename_parq = page_config['file_name'] + '_' + device + '_' + page_config['date'] + '.parquet'

            gcloud_filename = page_config['gcloud_path'] + page_config['date'] + '/' + filename
            gcloud_filename_parq = self.getParquetUploadFolder(page_config, filename_parq)

            gcloud_filename_ss = page_config['gcloud_path'].replace('crawl_data', 'crawl_ss') + page_config['date'] + '/' + filename.replace('.csv', '.jpg')

            df = pd.DataFrame(products_data)
            df = df.assign(full_page_snapshot = gcloud_filename_ss)
            df = df.reindex(columns= self.orderedColumns(df.columns.values.tolist()))
            df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", r"\\$"], value=["","",""], regex=True, inplace=True)
            # np.savetxt(filename, df.to_numpy(),fmt='%s', delimiter=':::')
            # self.bucket.blob(gcloud_filename).upload_from_filename(filename)
            self.bucket.blob(gcloud_filename_ss).upload_from_filename(img_path)

            df.drop(['country', 'retailer', 'date'], axis = 1, inplace = True)
            df.to_parquet(filename_parq, engine='fastparquet')
            self.bucket.blob(gcloud_filename_parq).upload_from_filename(filename_parq)
            
            if os.path.exists(img_path):
                os.remove(img_path)
            if os.path.exists(filename_parq):
                os.remove(filename_parq)
            # if os.path.exists(filename):
                # os.remove(filename)
                LOGGING.warn(page_config['retailer'] + ' ' + page_config['index'] + '/' + page_config['url_count'] + ' ' + str(len(products)))
        except Exception as e:
            LOGGING.error(e)
            self.pageError(page_config, 'parsePLPage exception', delete=img_path)
    
    def getParquetUploadFolder(self,config,filename):
        kpi = ''
        if 'crawl_data_se/' in config['gcloud_path']:
            kpi = 'SE'
        elif 'crawl_data/' in config['gcloud_path']:
            kpi = 'SOS'
        elif 'crawl_data_pdp/' in config['gcloud_path']:
            kpi = 'PDP'
            
        return 'crawl/{0}/country={1}/retailer={2}/date={3}/{4}'.format(
            kpi,
            config['country'],
            config['retailer'],
            config['date'],
            filename
        )

    def getProductsData(self, products, product_attrs, page_config):
        products_data = []
        position_counter = 1
        try:
            for product in products:
                product_data = {}
                for product_attr in product_attrs:
                    field = product_attr['field']
                    if field != 'position': 
                        product_data[field] = self.driver.fetchTextFromSelectors(product, product_attr['selectors'], page_config)
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

    def retailerWait(self, d, page_config, device):
        try:
            d.execute_script("window.scrollTo(0,document.body.scrollHeight);")
        except:
            LOGGING.error('Could now scroll to bottom')
            return
        try:    
            retailer = page_config['retailer']
            parser = self.parser_map[page_config['parsing_config']]
            if 'wait_for_class' in parser:
                WebDriverWait(d, RENDER_WAIT_LIMIT).until(EC.visibility_of_element_located((By.CLASS_NAME, parser['wait_for_class'])))
            elif 'wait_for_id' in parser:
                WebDriverWait(d, RENDER_WAIT_LIMIT).until(EC.visibility_of_element_located((By.ID, parser['wait_for_id'])))
            elif 'wait_for_class_device' in parser:
                WebDriverWait(d, RENDER_WAIT_LIMIT).until(EC.visibility_of_element_located((By.CLASS_NAME, parser['wait_for_class_device'][device])))
            elif 'wait_for_tag' in parser:
                    WebDriverWait(d, RENDER_WAIT_LIMIT).until(EC.visibility_of_element_located((By.TAG_NAME, parser['wait_for_tag'])))
            elif 'Sephora' in retailer:
                time.sleep(1.5)
            elif retailer in ['Myer']:
                time.sleep(2)
        except Exception as e:
            LOGGING.error('RetailerWait timeoutt')

        try:
            d.execute_script("window.scrollTo(0,0);")
        except:
            LOGGING.error('Could now scroll to top')
            return

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

    def driverCleaner(self):
        while self.consumerRunning:
            for proc in psutil.process_iter():
                try:
                    if proc.memory_percent() > MEMORY_THRESHOLD and 'chrome' in proc.name().lower():
                        pid = str(proc.pid)
                        flag = False
                        for active_driver in self.ACTIVE_DRIVERS:
                            if pid in active_driver["pids"]:
                                self.driver.quitDriver(active_driver["obj"])
                                flag = True
                                os.system('kill -9 ' + active_driver["pids"])
                                LOGGING.error('Driver Cleaner removed a chrome instance')
                                break
                        if flag:
                            self.activeDriverRemove(active_driver)
                        os.system('kill -9 ' + pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
                except Exception as e:
                    LOGGING.error(e)

            if len(self.ACTIVE_DRIVERS):
                for active_driver in self.ACTIVE_DRIVERS:
                    runtime = time.time() - active_driver["create_time"]
                    if runtime > DRIVER_CLEAN_TIME:
                        self.driver.quitDriver(active_driver["obj"])
                        os.system('kill -9 ' + active_driver["pids"])
                        self.activeDriverRemove(active_driver)
                        LOGGING.error('Driver Cleaner removed a chrome instance')
            time.sleep(DRIVER_CLEAN_TIME_WAIT)

    def activeDriverRemove(self, active_driver):
        if active_driver in self.ACTIVE_DRIVERS:
            try:
                self.driver.quitDriver(active_driver["obj"])
                os.system('kill -9 ' + active_driver["pids"])
            except:
                pass
            self.ACTIVE_DRIVERS.remove(active_driver)
    
    def pgrep(self, term, regex=False, full=True) -> List[psutil.Process]:
        procs = []
        for proc in psutil.process_iter(['pid', 'name', 'username', 'cmdline']):
            try:
                if full:
                    name = ' '.join(proc.cmdline())
                else:
                    name = proc.name()
                try:
                    if regex and re.search(term, name):
                        procs.append(proc)
                    elif term in name:
                        procs.append(proc)
                except psutil.NoSuchProcess:
                    pass
            except:
                pass
        return procs

    def driver_proc(self, driver):
        directory = driver.user_data_dir
        procs = self.pgrep(directory, full=True)
        procs.sort(key=lambda p: p.pid)
        chromes_pids = []
        for proc in procs:
            try:
                if 'chrome' in proc.name().lower():
                    chromes_pids.append(str(proc.pid))
            except:
                pass
        return ' '.join(chromes_pids)

    def pageError(self, page_config, msg, delete= 'dummy.svg'):
        page_config['message'] = msg
        mongo.addErrorDocument(self.crawl_folder, page_config, kpi = 'PL')
        print(page_config['page_url'] + ' ' + '0')
        LOGGING.warn(page_config['page_url'] + ' ' + '0')
        if os.path.exists(delete):
            os.remove(delete)

    def orderedColumns(self, cols):
        order = []
        for item in cols:
            if item in COLUMN_ORDER:
                order.append(COLUMN_ORDER.index(item))
            else:
                order.append(999)

        final = [x   for _, x in sorted(zip(order, cols))]
        return final

    