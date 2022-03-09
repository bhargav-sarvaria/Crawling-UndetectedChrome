from crawler import Crawler
from pyvirtualdisplay import Display
import os
import sys

crawler = Crawler(1)
if __name__ == '__main__':
    try:
        display = Display(visible=0, size=(1024, 768))
        display.start()
        crawler.crawlUrlsFromConfigPath(sys.argv[1])
        os.environ["HEADLESS"] = sys.argv[2]
        
        # crawler.crawlUrlsFromConfigPath('Retry_EU')
        # os.environ["HEADLESS"] = '0'
    except Exception as e:
        print(e)
        