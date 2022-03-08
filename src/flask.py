app = Flask(__name__)
@app.route('/crawlUrlsFromConfig', methods = ['POST'])
def crawlUrlsFromConfig():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        config_path = request.json['config_path']
        crawler.crawlUrlsFromConfig(config_path)
        return request.json
    else:
        return 'Content-Type not supported!'
@app.route('/')
def hello_world():
    try:
        data = ''
        crawl_url = 'https://www.macys.com'
        country = 'France'
        if crawl_url:
            # display = Display(visible=0, size=(800,600))
            # display.start()
            driver = driver.get_driver(country)
            driver.get(crawl_url)
            data = driver.page_source
            driver.save_screenshot('ss.png')
            # bucket.blob('ss').upload_from_filename('ss.png')
            driver.close()
            driver.quit()
            # display.stop()
            return send_file('./ss.png', mimetype='image/gif')
    except Exception as e:
        print(e)
        data = ''
    return data
  