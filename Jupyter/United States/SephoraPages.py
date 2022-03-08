#!/usr/bin/env python
# coding: utf-8

# In[1]:


import undetected_chromedriver.v2 as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time
import json


# In[11]:


driver = uc.Chrome()
base_url = 'https://www.sephora.com'
driver.get(base_url)


# In[12]:


departments = driver.find_elements(By.CLASS_NAME, 'css-1p9oei7.e65zztl0')
departments_texts = []
for department in departments:
    departments_texts.append(department.text)


# In[13]:


conf = {}
conf['country'] = 'United States'
conf['retailer'] = 'Sephora'
conf['parsing_config'] = './retailer_config/sephoracom_parsing.json'
conf['gcloud_path'] = 'crawl_data/United States/Sephora/'
page_configs = []
for did, department in enumerate(departments):
#     if did != 3:
#         continue
    action = ActionChains(driver)
    action.move_to_element(department).perform()
    time.sleep(0.5)
    containers = driver.find_elements(By.CLASS_NAME, 'css-8m9mm5')
    container = containers[did]
    splits = container.find_elements(By.CLASS_NAME, 'css-1kczxm6')
    for split in splits:
        categories = split.find_elements(By.CLASS_NAME, 'css-gzygfv')
        page_list = split.find_elements(By.CLASS_NAME, 'css-1yu4v6k')
        for cid, category in enumerate(categories):
            
            # Adding the category landing page itself
            if len(category.find_elements(By.TAG_NAME, 'a')):
                page_config = conf.copy()            
                page_config['department'] = departments_texts[did].strip()
                page_config['category'] = category.text.strip()
                page_config['page'] = category.text.strip()
                page_config['page_url'] = category.find_element(By.TAG_NAME, 'a').get_attribute('href')
                file_name = page_config['country'] + '_' + page_config['retailer'] + '_' + page_config['department'] + '_' + page_config['category'] + '_' + page_config['page']
                for k, v in json.load(open('config/encoding.json', 'r')).items():
                    file_name = file_name.replace(k, v)
                    file_name = file_name.replace('\\', '')
                page_config['file_name'] = file_name
                page_configs.append(page_config)
            
            pages = page_list[cid].find_elements(By.CLASS_NAME, 'css-ri25u8.eanm77i0')
            for page in pages:
                page_config = conf.copy()
                page_config['department'] = departments_texts[did].strip()
                page_config['category'] = category.text.strip()
                page_config['page'] = page.text.strip()
                page_config['page_url'] = page.get_attribute('href')
                file_name = page_config['country'] + '_' + page_config['retailer'] + '_' + page_config['department'] + '_' + page_config['category'] + '_' + page_config['page']
                for k, v in json.load(open('config/encoding.json', 'r')).items():
                    file_name = file_name.replace(k, v)
                    file_name = file_name.replace('\\', '')
                page_config['file_name'] = file_name
                page_configs.append(page_config)
                
        directs = split.find_elements(By.XPATH, "./a")
        for direct in directs:
            page_config = conf.copy()
            page_config['department'] = departments_texts[did].strip()
            page_config['category'] = direct.text
            page_config['page'] = direct.text
            page_config['page_url'] = direct.get_attribute('href')
            file_name = page_config['country'] + '_' + page_config['retailer'] + '_' + page_config['department'] + '_' + page_config['category'] + '_' + page_config['page']
            for k, v in json.load(open('config/encoding.json', 'r')).items():
                file_name = file_name.replace(k, v)
                file_name = file_name.replace('\\', '')
            page_config['file_name'] = file_name
            page_configs.append(page_config)
            
            


# In[8]:


with open("sephoracom_urls.json", "w") as outfile:
        json.dump(page_configs, outfile)


# In[9]:


driver.close()
driver.quit()

