# CREDIT: slightly changed version of this https://github.com/Gertje823/Vinted-Scraper/blob/main/scraper.py 

# importing the requests library 
import requests
import json
from fake_useragent import UserAgent 
import os
import time
from datetime import datetime



def scrape_user(user_ids):
    # user_ids = [47015621]

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


        # DEBUG
        # print(jsonresponse)
        # f = open("./vinted_raw.json", "w")
        # f.write(str(str(jsonresponse).encode("utf-8")))
        # f.close()


        for json_items in jsonresponse['items']:
                # check which
                #  gender by checking item url
                # print(json_items['url'].find("femme"))
                item_gender =  ''
                if json_items['url'].find("femme") != -1: 
                    item_gender =  'female'
                else: 
                    item_gender =  'male'

                # print(json.dumps(json_items['user']['followers_count']))
                # time.sleep(1)

                item_data['items'].append({ "user_id": json_items['user_id'],
                                            "user_login": json_items['user_login'],

                                            "following_count": json_items['user']['following_count'],
                                            "followers_count": json_items['user']['followers_count'],
                                            "total_items_count": json_items['user']['total_items_count'],
                                            "avg_response_time": json_items['user']['avg_response_time'],
                                            "volunteer_moderator": json_items['user']['volunteer_moderator'],
                                            "closet_promoted_until": json_items['user']['closet_promoted_until'],
                                            "profile_url": json_items['user']['profile_url'],
                                            "is_online": json_items['user']['is_online'],
                                            "has_promoted_closet": json_items['user']['has_promoted_closet'],
                                            "about": json_items['user']['about'],
                                            "feedback_reputation": json_items['user']['feedback_reputation'],
                                            "is_shadow_banned": json_items['user']['is_shadow_banned'],
                                            "negative_feedback_count": json_items['user']['negative_feedback_count'],
                                            "country_iso_code": json_items['user']['country_iso_code'],
                                            "city_id": json_items['user']['city_id'],
                                            "city_name": json_items['user']['city'],
                                            "country_title_local": json_items['user']['country_title_local'],
                                            "country_title": json_items['user']['country_title'],
                                            "is_hated": json_items['user']['is_hated'],
                                            "avg_response_time": json_items['user']['avg_response_time'],

                                            "item_id": json_items['id'],
                                            "title": json_items['title'],
                                            "price": json_items['price'],
                                            "description": json_items['description'],
                                            "brand": json_items['brand'],
                                            "label": json_items['label'],
                                            "size": json_items['size'],
                                            "gender": item_gender,
                                            "color1": json_items['color1'],
                                            "status": json_items['status'],
                                            "photos": json_items['photos'],
                                            "catalog_id": json_items['catalog_id'],
                                            "url": json_items['url'],
                                            "reserved_for_user_id": json_items['reserved_for_user_id'],
                                            "favourites": json_items['favourite_count'],
                                            "view_count": json_items['view_count'],
                                            "is_admin_alerted": json_items['is_admin_alerted'],
                                            "active_bid_count": json_items['active_bid_count'],
                                            
                                            "date_scraped": {
                                                "h": datetime.now().strftime("%H"), 
                                                "m": datetime.now().strftime("%M"),  
                                                "s": datetime.now().strftime("%S")  
                                                }

                                            }) 
    return item_data 


# read local config file 
# f = open (os.path.dirname(os.path.realpath(__file__)) + '\\config\\vinted_scraper_config.json')
  
# Reading from file 
# config = json.loads(f.read())

# print(config['user_ids'][0])
user_ids = [47015621] 
# 44205571,46942432,44918503,45161108,52931034,37142759,43942636,44332099]
print(json.dumps(scrape_user(user_ids)['items'][0]['date_scraped']))