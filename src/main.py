from crawler import Crawler
from crawler_pdp import Crawler_PDP
from pyvirtualdisplay import Display
import os
import sys
import atexit

if sys.platform.endswith("linux"):
    DISPLAY = Display(visible=0, size=(1024, 768))

def exit_handler():
    if sys.platform.endswith("linux"):
        DISPLAY.stop()

if __name__ == '__main__':
    try:
        atexit.register(exit_handler)
        if sys.platform.endswith("linux"):
            DISPLAY.start()

        os.environ["HEADLESS"] = sys.argv[2]
        if sys.argv[4] == 'PL':
            Crawler(int(sys.argv[3])).crawlUrlsFromConfigPath(sys.argv[1])
        elif sys.argv[4] == 'PDP':
            Crawler_PDP(int(sys.argv[3])).crawlUrlsFromConfigPath(sys.argv[1])
            
        # os.environ["HEADLESS"] = '0'
        # crawler = Crawler(1)
        # crawler.crawlUrlsFromConfigPath('United States')
        
    except Exception as e:
        print(e)
        