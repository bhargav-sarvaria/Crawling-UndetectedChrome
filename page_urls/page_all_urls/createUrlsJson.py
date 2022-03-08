import json

with open('bloomingdales.com.json', 'r') as f:
    configs = []
    f = json.load(f)
    for department in f:
        for category in f[department]:
            for page in f[department][category]:
                config = {
                    "country": "US",
                    "retailer": "Bloomingdales",
                    "department": department,
                    "category": category,
                    "page": page,
                    "page_url": f[department][category][page]
                }
                file_name = config['country'] + '_' + config['retailer'] + '_' + department + '_' + category + '_' + page
                for k, v in json.load(open('config/encoding.json', 'r')).items():
                    file_name = file_name.replace(k, v)
                    file_name = file_name.replace('\\', '')
                config['file_name'] = file_name
                config['parsing_config'] = "./retailer_config/bloomingdalescom_parsing.json"
                config['gcloud_path'] = "crawl_data/"+config['country']+"/"+config['retailer']+"/"
                configs.append(config)
    print(configs[-1:])
    with open("bloomingdalescom_urls.json", "w") as outfile:
        json.dump(configs, outfile)