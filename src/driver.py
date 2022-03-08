import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from importlib.machinery import SourceFileLoader

uc = SourceFileLoader("undetected_chromedriver", "./chrome/packages/undetected_chromedriver/__init__.py").load_module()
proxies_file = './chrome/proxies/proxies.txt'
extension_folder = './chrome/extensions/'
TOTAL_PROXIES = 50

class Driver:
    def __init__(self):
        self.proxy_index = 0
        with open(proxies_file) as file:
            self.proxies = [line.rstrip() for line in file]

    def getProxy(self):
        self.proxy_index += 1
        return self.proxies[self.proxy_index]

    def get_driver(self, country_name='United States'):
        try:
            opt = uc.ChromeOptions()
            opt.add_argument("--no-sandbox")
            opt.add_argument("--disable-dev-shm-usage")
            opt.add_argument("--start-maximized")
            opt.add_argument("--disable-infobars")
            opt.add_argument('--disable-gpu')
            opt.add_argument('--blink-settings=imagesEnabled=false')
            opt.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
            # opt.add_argument('--load-extension='+extension_folder+'hoxx')
            opt.add_argument('--load-extension='+extension_folder+'hoxx,'+extension_folder+'CSS-block,'+extension_folder+'GIF-blocker')
            driver = uc.Chrome(options = opt, use_subprocess=True)
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
                        return driver
        except Exception as e:
            print(e)
        return None

    def get_driverWithoutHoxx(self, use_proxy=False):
        try:
            opt = uc.ChromeOptions()
            if use_proxy:
                proxy = self.getProxy()
                opt.add_argument('--proxy-server=%s' % proxy)
            opt.add_argument("--no-sandbox")
            opt.add_argument("--disable-dev-shm-usage")
            opt.add_argument("--start-maximized")
            opt.add_argument("--lang=en")
            opt.add_argument("--disable-infobars")
            if os.environ['HEADLESS'] == '1':
                opt.add_argument("--headless")
            opt.add_argument('--blink-settings=imagesEnabled=false')
            opt.add_argument('--load-extension='+extension_folder+'CSS-Block,'+ extension_folder+'GIF-blocker')
            
            opt.add_argument('--disable-gpu')
            opt.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
            opt.add_argument('--referer=https://www.google.com/')
            # Kiosk is for full screen
            # opt.add_argument("--kiosk")
            
            driver = uc.Chrome(options = opt)
            driver.set_page_load_timeout(10)
            return driver
        except Exception as e:
            print(e)
        return None
