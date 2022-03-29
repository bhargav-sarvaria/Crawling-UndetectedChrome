from crawler import Crawler
from pyvirtualdisplay import Display
import os
import sys

crawler = Crawler(2)
if __name__ == '__main__':
    try:
        if sys.platform.endswith("linux"):
            display = Display(visible=0, size=(1024, 768))
            display.start()
        crawler.crawlUrlsFromConfigPath(sys.argv[1])
        os.environ["HEADLESS"] = sys.argv[2]
        
        # crawler.crawlUrlsFromConfigPath('United Kingdom')
        # os.environ["HEADLESS"] = '0'
    except Exception as e:
        print(e)
        