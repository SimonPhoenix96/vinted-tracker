# CREDIT: slightly changed version of this https://github.com/Gertje823/Vinted-Scraper/blob/main/scraper.py 

# importing the requests library 
import requests
import json
from fake_useragent import UserAgent 
import os
import time




user_ids = [47015621]

s = requests.Session()
ua = UserAgent()
s.headers = {
    'User-Agent': ua.firefox,
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en',
    'DNT': '1',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}
req = s.get("https://www.vinted.de/member/44787336-mavkalis")
# print(req.text.find("csrf-token"))
csrfToken = req.text.split('<meta name="csrf-token" content="')[1].split('"')[0]
s.headers['X-CSRF-Token'] = csrfToken
params = (
    ('localize', 'true'),
)


item_data = {}
item_data['items'] = []

for USER_ID in user_ids:
    # USER_ID = USER_ID.strip('\n')
    # print(USER_ID)
    url = f'https://www.vinted.de/api/v2/users/{USER_ID}/items?page=1&per_page=200000'
    # print('ID=' + str(USER_ID))

    r = s.get(url)
    jsonresponse = r.json()
    json_items = jsonresponse['items']

    # print(jsonresponse)
    f = open("./vinted_raw.json", "w")
    f.write(str(str(jsonresponse).encode("utf-8")))
    f.close()

    for json_items in jsonresponse['items']:

            item_data['items'].append({"user_id": json_items['user_id'],
                                        "catalog_id": json_items['catalog_id'],
                                       'item_id': json_items['id'],
                                        "title": json_items['title'],
                                        "price": json_items['price'],
                                        "description": json_items['description'],
                                        "brand": json_items['brand'],
                                        "size": json_items['size'],
                                        "color1": json_items['color1'],
                                        "status": json_items['status'],
                                        "photos": json_items['photos'],
                                        "url": json_items['url']
                                         }) 
            # check which gender by checking item url
            # print(json_items['url'].find("femme"))
            if json_items['url'].find("femme") != -1: 
                item_data['items'].append({"gender": 'female'})
            else: item_data['items'].append({"gender": 'male'})



            # item_data['items'].append({"item_id": json_items['id']})  
           # item_data.update({'item_id': json_items['id']}) 
            # item_data['items'].append({"title": json_items['title']})  
            # item_data['items'].append({"price": json_items['price']})   
            # item_data['items'].append({"description": json_items['description']})   
            
            # # item_data['items'].append({"gender": json_items['gender']})   
            # item_data['items'].append({"catalog_id": json_items['catalog_id']})   
            # item_data['items'].append({"size": json_items['size']})   
            # item_data['items'].append({"status": json_items['status']})   
            # item_data['items'].append({"brand": json_items['brand']})   
            # item_data['items'].append({"color1": json_items['color1']})   
            # item_data['items'].append({"photos": json_items['photos']})   


 









