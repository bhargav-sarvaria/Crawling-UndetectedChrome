import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from importlib.machinery import SourceFileLoader
from PIL import Image
import logging as LOGGING
import json
from lxml import html

LOGGING.basicConfig(
filename='run.log',
filemode='a',
format='[%(asctime)s] %(levelname)s {%(filename)s:%(lineno)d} -  %(message)s',
datefmt='%H:%M:%S',
level=LOGGING.WARN
)

uc = SourceFileLoader("undetected_chromedriver", "./chrome/packages/undetected_chromedriver/__init__.py").load_module()
proxies_file = './chrome/proxies/proxies.txt'
extension_folder = './chrome/extensions/'
TOTAL_PROXIES = 50

PREFS = {'profile.default_content_setting_values': {'images': 2, 'plugins': 2, 'popups': 2, 'geolocation': 2, 'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2,'mouselock': 2, 'mixed_script': 2, 'media_stream': 2, 'media_stream_mic': 2, 'media_stream_camera': 2, 'protocol_handlers': 2, 'ppapi_broker': 2, 'automatic_downloads': 2, 'midi_sysex': 2, 'push_messaging': 2, 'ssl_cert_decisions': 2, 'metro_switch_to_desktop': 2, 'protected_media_identifier': 2, 'app_banner': 2, 'site_engagement': 2, 'durable_storage': 2}, "intl.accept_languages": "en,en-US,en-GB"}

class Driver:
    def __init__(self):
        self.proxy_index = 0
        with open(proxies_file) as file:
            self.proxies = [line.rstrip() for line in file]

    def getProxy(self):
        self.proxy_index += 1
        return self.proxies[self.proxy_index]

    def get_driver(self, 
        country_name='United States', 
        hoxx = False, 
        timeout = 10,
        device = 'Desktop',
        use_proxy=False
    ):
        try:
            opt = uc.ChromeOptions()
            opt.add_argument("--disable-dev-shm-usage")
            opt.add_argument("--start-maximized")
            opt.add_argument("--disable-infobars")
            opt.add_argument("--disable-gpu")
            opt.add_argument("--enable-strict-powerful-feature-restrictions")
            opt.add_experimental_option("prefs", PREFS)

            if use_proxy:
                proxy = self.getProxy()
                opt.add_argument('--proxy-server=%s' % proxy)

            
            if device == 'Mobile':
                opt.add_argument('--user-agent=Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1')

            else:
                opt.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36")
            
            if hoxx:
                opt.add_argument('--load-extension='+extension_folder+'hoxx')

            if os.environ['HEADLESS'] == '1':
                opt.add_argument("--headless")

            driver = uc.ChromeWithPrefs(options = opt, use_subprocess=True)
            driver.set_page_load_timeout(timeout)

            if hoxx:
                handles = driver.window_handles
                while len(handles) != 2:
                    handles = driver.window_handles
                for idx,handle in enumerate(handles):
                    if idx != 0:
                        driver.switch_to.window(handle)
                        driver.close()
                driver.switch_to.window(handles[0])
                driver.get('chrome-extension://nbcojefnccbanplpoffopkoepjmhgdgh/popup.html')
                language_flag = False
                loggedin_flag = False
                WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, 'mdc-list-item')))
                languages = driver.find_elements(By.CLASS_NAME, 'mdc-list-item')
                for language in languages:
                    if language.text == 'English':
                        language.click()
                        language_flag = True
                        break
                    elif language.text == 'India':
                        loggedin_flag = True
                        break

                if language_flag:
                    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, 'email-input')))
                    driver.find_element(By.ID, 'email-input').click()
                    action = ActionChains(driver)
                    action.send_keys('ttsst132@gmail.com')
                    action.perform()
                    driver.find_element(By.ID, 'password-input').click()
                    action = ActionChains(driver)
                    action.send_keys('bhargav19')
                    action.perform()
                    driver.find_element(By.ID, 'login-button').click()
                    loggedin_flag = True

                if loggedin_flag:
                    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, 'mdc-list-item')))
                    countries = driver.find_elements(By.CLASS_NAME, 'mdc-list-item')
                    for country in countries:
                        if country.text == country_name:
                            driver.execute_script("arguments[0].scrollIntoView();", country)
                            country.click()
                            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, 'connected-view__status-title')))
                            # print('Created the final driver for Country: ' + country_name)
                            break
            if device == 'Mobile':
                driver.set_window_size(390, 844)
            return driver
        except Exception as e:
            print(e)
        return None   
    
    def save_screenshot(self, driver, path) -> None:
        try:
            path = path.replace('.jpg', '.png')
            original_size = driver.get_window_size()
            required_width = driver.execute_script('return document.body.parentNode.scrollWidth')
            required_height = driver.execute_script('return document.body.parentNode.scrollHeight')
            driver.set_window_size(required_width, min(6000, required_height))
            try:
                driver.find_element_by_tag_name('body').screenshot(path)
            except:
                driver.save_screenshot(path);
            self.compressPngToJpg(path)
            driver.set_window_size(original_size['width'], original_size['height'])
        except Exception as e:
            return

    def compressPngToJpg(self, img_path):
        try:
            im = Image.open(img_path)
            jpg_img_path = img_path.replace('.png', '.jpg')
            rgb_im = im.convert('RGB')
            rgb_im.save(jpg_img_path)
            os.remove(img_path)
            im = Image.open(jpg_img_path)
            im.save(jpg_img_path,optimize=True,quality=30) 
        except:
            return

    def quitDriver(self, d):
        try:
            d.close()
        except Exception as e:
            LOGGING.error(e)
            LOGGING.error('Could not close driver')
        
        try:
            d.quit()
        except Exception as e:
            LOGGING.error(e)
            LOGGING.error('Could not quit driver')

    def getDriverElements(self, d, selector):
        elements = []
        try:
            if selector['type'] == 'classname':
                elements = d.find_elements(By.CLASS_NAME, selector['value'])
        except:
            elements = []
        return elements

    def deleteDriverElements(self, d, selector):
        elements = []
        try:
            if selector['type'] == 'classname':
                elements = d.find_elements(By.CLASS_NAME, selector['value'])
        except:
            elements = []
        for el in elements:
            try:
                d.execute_script("arguments[0].remove();", el)
            except:
                pass
        return

    def fetchSeleniumElements(self, element, selectors):
        elements = []
        for selector in selectors:
            try:
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
            except:
                pass
            if len(elements) > 0:
                return elements
        return elements

    def fetchSoupElements(self, element, selectors):
        elements = []
        for selector in selectors:
            try:
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
            except:
                pass
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
                elif selector['type'] == 'classname_value_flag':
                    value = '0'
                    if element.find(class_= selector['classname']):
                        if element.find(class_= selector['classname']).getText().lower().strip() == selector['value'].lower():
                            value = '1'

                
            except:
                value = ''
                            
            if value is not None and value!= '':
                return value.strip()
            else:
                value = ''
                continue
        return '' 