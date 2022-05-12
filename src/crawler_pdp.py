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
from selenium.common.exceptions import TimeoutException,StaleElementReferenceException
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
import itertools
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

COLUMN_ORDER = ["country","retailer","brand","product_name","crawl_brand","crawl_product_name","sku", "oos", "product_page_url","full_page_snapshot","date"]


class Crawler_PDP:
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
                t = threading.Thread(target = self.processPDPConfig, name=thread_name, args=(confs,thread_name,))
                t.start()
            else:
                time.sleep(5)

    def crawlUrlsFromConfigPath(self, crawl_folder):
        if 'Retry' in crawl_folder:
            RENDER_WAIT_LIMIT = 10
            self.crawl_folder = crawl_folder.split('_')[1]
            crawl_urls = mongo.getDocumentsForRetry(crawl_folder, kpi = 'PDP')

            for idx, page_config in enumerate(crawl_urls):
                if page_config['parsing_config'] not in self.parser_map:
                    self.parser_map[page_config['parsing_config']] = json.load(open(page_config['parsing_config'],'r'))
                page_config['index'] = str(idx)
                page_config['url_count'] = str(len(crawl_urls))
                self.addConfig(page_config)
        else:
            self.crawl_folder = crawl_folder
            crawl_path = './pdp_urls/' + crawl_folder + '/'
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

    def processPDPConfig(self, page_configs,thread_name):
        use_proxy = False
        timeout = 10
        if 'Proxy' in self.crawl_folder:
            use_proxy = True
        d = self.driver.get_driver(use_proxy=use_proxy, timeout=timeout)
        active_driver = self.get_activeDriver(d)
        self.ACTIVE_DRIVERS.append(active_driver)
        for page_config in page_configs:
            try:
                try:
                    d.get(page_config['product_page_url'])
                except  TimeoutException:
                    pass
                except Exception as e:
                    LOGGING.error(e)
                    self.driver.quitDriver(d)
                    self.activeDriverRemove(active_driver)
                    d = self.driver.get_driver(use_proxy=use_proxy, timeout=timeout)
                    active_driver = self.get_activeDriver(d)
                    self.ACTIVE_DRIVERS.append(active_driver)
                    d.get(page_config['product_page_url'])
                
                # Wait for lazy loading
                self.retailerWait(d, page_config)
                # self.translateToEnglish(d)

                parser = self.parser_map[page_config['parsing_config']]
                variant_selectors = []
                variants = parser['variants']
                delete_elements = parser['deletes']
                product_attrs = parser['details']

                for element in delete_elements:
                    self.driver.deleteDriverElements(d, element)
                    
                for variant in variants:
                    filters = self.driver.getDriverElements(d, variant['selector'])
                    for filt in filters:
                        variant_selectors.append(self.driver.getDriverElements(filt, variant['variant_values']))
                final_list = []
                if len(variant_selectors) > 0:
                    for element in itertools.product(*variant_selectors):
                        final_list.append(list(element))

                product_data = []
                if len(final_list) > 0:
                    for combination in final_list:
                        combination_str = []
                        for item in combination:
                            try:
                                combination_str.append(item.get_attribute('textContent').strip())
                                d.execute_script ("arguments[0].click();", item)
                            except StaleElementReferenceException:
                                continue
                        time.sleep(2)
                        for element in delete_elements:
                            self.driver.deleteDriverElements(d, element)
                        combination_str =  '_'.join(combination_str)
                        img_path = './' + str(time.time())+ '.jpg'
                        self.driver.save_screenshot(d, img_path)
                        ss_filename = page_config['file_name'] + '_' + combination_str.replace('/', '|') + '_' + page_config['date'] + '.jpg'
                        gcloud_ss_filename = page_config['gcloud_path'].replace('crawl_data', 'crawl_ss') + page_config['date'] + '/' + ss_filename
                        self.bucket.blob(gcloud_ss_filename).upload_from_filename(img_path)
                        page_config['sku'] = combination_str
                        page_config['full_page_snapshot'] = gcloud_ss_filename
                        page = BeautifulSoup(d.page_source, 'html.parser')
                        sku = self.getSkuData(page, product_attrs, page_config)
                        product_data.append(sku)
                        if os.path.exists(img_path):
                            os.remove(img_path)
                else:
                    img_path = './' + str(time.time())+ '.jpg'
                    self.driver.save_screenshot(d, img_path)
                    ss_filename = page_config['file_name'] + '_' + page_config['date'] + '.jpg'
                    gcloud_ss_filename = page_config['gcloud_path'].replace('crawl_data', 'crawl_ss') + page_config['date'] + '/' + ss_filename
                    self.bucket.blob(gcloud_ss_filename).upload_from_filename(img_path)
                    page_config['sku'] = 'None'
                    page_config['full_page_snapshot'] = gcloud_ss_filename
                    page = BeautifulSoup(d.page_source, 'html.parser')
                    sku = self.getSkuData(page, product_attrs, page_config)
                    product_data.append(sku)
                    if os.path.exists(img_path):
                        os.remove(img_path)
                
                self.savePDPData(product_data, page_config)

            except Exception as e:
                LOGGING.error(e)
                self.pageError(page_config, 'processPDPConfig exception')
        
        self.driver.quitDriver(d)
        self.activeDriverRemove(active_driver)
    
        self.RUNNING_THREADS.remove(thread_name)
        if len(self.RUNNING_THREADS) == 0 and not self.queueHasItems():
            self.consumerRunning = False

    def getSkuData(self, page, product_attrs, config):
        sku_data = {}
        for product_attr in product_attrs:
            field = product_attr['field']
            if field == 'oos':
                elements = self.driver.fetchSeleniumElements(page, product_attr['selectors'])
                present = len(elements) > 0
                if present == product_attr['selectors'][0]['present']:
                    sku_data[field] = '1'
                else:
                    sku_data[field] = '0'
            else:
                sku_data[field] = self.driver.fetchTextFromSelectors(page, product_attr['selectors'], config)
        return sku_data
    
    def savePDPData(self, product_data, page_config):
        try:
            df = pd.DataFrame(product_data)
            df = df.reindex(columns= self.orderedColumns(df.columns.values.tolist()))
            df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", r"\\$"], value=["","",""], regex=True, inplace=True)
            filname = page_config['file_name'] + '_' + page_config['date'] + '.csv'
            np.savetxt(filname, df.to_numpy(),fmt='%s', delimiter=':::')
            gcloud_filename = page_config['gcloud_path'] + page_config['date'] + '/' + filname
            self.bucket.blob(gcloud_filename).upload_from_filename(filname)

            filname_parq = page_config['file_name'] + '_' + page_config['date'] + '.parquet'
            df.drop(['country', 'retailer', 'date'], axis = 1, inplace = True)
            df.to_parquet(filname_parq, engine='fastparquet')
            gcloud_filename_parq = self.getParquetUploadFolder(page_config, filname_parq)
            self.bucket.blob(gcloud_filename_parq).upload_from_filename(filname_parq)
            
            if os.path.exists(filname):
                os.remove(filname)
            if os.path.exists(filname_parq):
                os.remove(filname_parq)
                print(page_config['product_name'] + ' ' + page_config['index'] + '/' + page_config['url_count'])
                LOGGING.warn(page_config['product_name'] + ' ' + page_config['index'] + '/' + page_config['url_count'])
        except Exception as e:
            LOGGING.error(e)
            self.pageError(page_config, 'parsePLPage exception')

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
    
    def retailerWait(self, d, page_config, device='Desktop'):
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
        mongo.addErrorDocument(self.crawl_folder, page_config, kpi = 'PDP')
        print(page_config['product_page_url'] + ' ' + '0')
        LOGGING.warn(page_config['product_page_url'] + ' ' + 'error')
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