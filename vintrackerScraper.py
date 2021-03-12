# CREDIT: slightly changed version of this https://github.com/Gertje823/Vinted-Scraper/blob/main/scraper.py 

# importing the requests library 
import requests
import json
from fake_useragent import UserAgent 
import os
import time
from datetime import datetime

def get_symbol(price):
        import re
        pattern =  r'(\D*)[\d\,\.]+(\D*)'
        g = re.match(pattern, price.strip()).groups()
        return (g[0] or g[1]).strip()


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
 
    user_data = {}
    user_data['users'] = []

    for USER_ID in user_ids:
        # USER_ID = USER_ID.strip('\n')
        # print(USER_ID)
        url = f'https://www.vinted.de/api/v2/users/{USER_ID}/items?page=1&per_page=200000'
        # print('ID=' + str(USER_ID))

        req = s.get("https://www.vinted.de/member/44787336-mavkalis")
        # print(req.text.find("csrf-token"))
        csrfToken = req.text.split('<meta name="csrf-token" content="')[1].split('"')[0]
        req = ""
        s.headers['X-CSRF-Token'] = csrfToken
        params = (
            ('localize', 'true'),
        )

        r = s.get(url)
        jsonresponse = r.json()
        json_items = jsonresponse['items']


        # DEBUG
        # print(jsonresponse)
        # f = open("./vinted_raw.json", "w")
        # f.write(str(str(jsonresponse).encode("utf-8")))
        # f.close()


        items = {}
        items['items'] = []

        # print(json.dumps(user_data))
        

        for json_items in jsonresponse['items']:
                # check which
                #  gender by checking item url
                # print(json_items['url'].find("femme"))
                item_gender =  ''
                if json_items['url'].find("femme") != -1: 
                    item_gender =  'female'
                else: 
                    item_gender =  'male'

                # extract currency symbol from item price
                currency_symbol = get_symbol(json_items['price'])


                # OLD :  keys in items are item_id 
                items['items'].append({                                           

                                "item_id": json_items['id'],

                                "initially_scraped":  datetime.now(),

                                "user_id": jsonresponse['items'][0]['user_id'],
                                "title": json_items['title'],
                                "currency_symbol": currency_symbol,
                                "price": json_items['price'].replace(currency_symbol, ''),
                                "description": json_items['description'],
                                "brand": json_items['brand'],
                                "label": json_items['label'],
                                "size": json_items['size'],
                                "gender": item_gender,
                                "color1": json_items['color1'],
                                "status": json_items['status'],
                                # json_items['photos']
                                "photos": "x",
                                "catalog_id": json_items['catalog_id'],
                                "url": json_items['url'],
                                "reserved_for_user_id": json_items['reserved_for_user_id'],
                                "favourites": json_items['favourite_count'],
                                "view_count": json_items['view_count'],
                                "is_admin_alerted": json_items['is_admin_alerted'],
                                "active_bid_count": json_items['active_bid_count'],
                                
                            
                })
        # OLD : keys in users are user_ids
        user_data['users'].append({ 
            
                                    "user_id": jsonresponse['items'][0]['user_id'],

                                    "initially_scraped":  datetime.now(),
                                            
                                    "user_login": jsonresponse['items'][0]['user_login'],
                                    "following_count": jsonresponse['items'][0]['user']['following_count'],
                                    "followers_count": jsonresponse['items'][0]['user']['followers_count'],
                                    "total_items_count": jsonresponse['items'][0]['user']['total_items_count'],
                                    "avg_response_time": jsonresponse['items'][0]['user']['avg_response_time'],
                                    "volunteer_moderator": jsonresponse['items'][0]['user']['volunteer_moderator'],
                                    "closet_promoted_until": jsonresponse['items'][0]['user']['closet_promoted_until'],
                                    "profile_url": jsonresponse['items'][0]['user']['profile_url'],
                                    "is_online": jsonresponse['items'][0]['user']['is_online'],
                                    "has_promoted_closet": jsonresponse['items'][0]['user']['has_promoted_closet'],
                                    "about": jsonresponse['items'][0]['user']['about'],
                                    "feedback_reputation": jsonresponse['items'][0]['user']['feedback_reputation'],
                                    "is_shadow_banned": jsonresponse['items'][0]['user']['is_shadow_banned'],
                                    "negative_feedback_count": jsonresponse['items'][0]['user']['negative_feedback_count'],
                                    "country_iso_code": jsonresponse['items'][0]['user']['country_iso_code'],
                                    "city_id": jsonresponse['items'][0]['user']['city_id'],
                                    "city_name": jsonresponse['items'][0]['user']['city'],
                                    "country_title_local": jsonresponse['items'][0]['user']['country_title_local'],
                                    "country_title": jsonresponse['items'][0]['user']['country_title'],
                                    "is_hated": jsonresponse['items'][0]['user']['is_hated'],

                                    "date_scraped": [{

                                                        "h": datetime.now().strftime("%H"), 
                                                        "m": datetime.now().strftime("%M"),  
                                                        "s": datetime.now().strftime("%S")  
                                                            
                                                    }],

                                    "items": items['items']


                                })

    # for user in user_data['users']:
    #     print(json.dumps(user))
        # if 
        # user['items'].append(items['items'])
    return user_data 

def main():
    
    # read local config file 
    # f = open (os.path.dirname(os.path.realpath(__file__)) + '\\config\\vinted_scraper_config.json')
    
    # Reading from file 
    # config = json.loads(f.read())

    # print(config['user_ids'][0])
    user_ids = [47015621,44332099, 44787336] 
    # 44205571,46942432,44918503,45161108,52931034,37142759,43942636,44332099]
    user_data = scrape_user(user_ids)
    for user in user_data['users']:
        print(json.dumps(user, default=str))

# main()    