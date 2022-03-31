from crawler import Crawler
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
        Crawler(int(sys.argv[3])).crawlUrlsFromConfigPath(sys.argv[1])
        
        # os.environ["HEADLESS"] = '0'
        # crawler = Crawler(1)
        # crawler.crawlUrlsFromConfigPath('Retry_United States_Mobile')
        
    except Exception as e:
        print(e)
        