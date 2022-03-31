from crawler import Crawler
from pyvirtualdisplay import Display
import os
import sys
import atexit

if sys.platform.endswith("linux"):
    DISPLAY = Display(visible=0, size=(1024, 768))

crawler = Crawler(2)

def exit_handler():
    if sys.platform.endswith("linux"):
        DISPLAY.stop()

if __name__ == '__main__':
    try:
        atexit.register(exit_handler)
        if sys.platform.endswith("linux"):
            DISPLAY.start()
        os.environ["HEADLESS"] = sys.argv[2]
        crawler.crawlUrlsFromConfigPath(sys.argv[1])
        
        # os.environ["HEADLESS"] = '0'
        # crawler.crawlUrlsFromConfigPath('BU')
        
    except Exception as e:
        print(e)
        