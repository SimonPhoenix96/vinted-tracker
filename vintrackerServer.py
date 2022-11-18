import json
import vintrackerScraper
import vintrackerDatabase
import os
import copy
from datetime import timedelta

from tasks import add

def vintrackerServer():
    
    # # read local config file
    f = open(os.path.dirname(os.path.realpath(__file__)) +
             '/Config/vinted_database_config.json')

    # # reading config from file
    config = json.loads(f.read())
    
    # # create database engine
    engine = vintrackerDatabase.get_database_engine(
        config['pg_server_admin'],
        config['pg_server_admin_password'],
        config['pg_server_ip'],
        "vintracker_database",
        False)

    # # vintracker first time database setup
    vintrackerDatabase.create_default_database(engine)
    vintrackerDatabase.create_default_tables(engine)
    vintrackerDatabase.create_default_users(
        config['vintracker_admin'],
        config['vintracker_admin_pw'],
        config['vintracker_scraper'],
        config['vintracker_scraper_pw'],
        engine,
        False)

    connection = engine.connect()
    # print(config['user_ids'][0])
    user_ids = [79562987] 
    # 44205571,46942432,44918503,45161108,52931034,37142759,43942636,44332099]
    vinted_data = vintrackerScraper.scrape_user(user_ids)

    vinted_data_copy = copy.deepcopy(vinted_data) # TODO fix deepcopy not working pass by reference hard to do in python

    # # # get item and user change
    item_changes = dict()
    for user in vinted_data_copy['users']:
        item_changes = vintrackerDatabase.get_scraped_item_data_difference(user['items'], connection)
    
    if item_changes is not None:
        for item_change in item_changes: 
            print("Inserting Changes!")
            vintrackerDatabase.insert_item_change(item_change,connection)
    else:
        print("No Item Changes to add!")

    # user_changes = dict()
    # user_changes = vintrackerDatabase.get_scraped_user_data_difference(vinted_data['users'], connection)
    
    # if user_changes is not None:
    #     for user_change in user_changes: 
    #         print(user_change)
    #         # vintrackerDatabase.insert_user_change(user_change,connection)
    # else:
    #     print("No User Changes to add!")



    # # # insert users to db
    for user in vinted_data['users']:
        # print( user  )
        vintrackerDatabase.insert_user_data(user, connection)

    
    # # insert every scraped users items to db
    for user in vinted_data['users']:
        for item in user['items']:
            vintrackerDatabase.insert_item_data(item, connection)

    # insert every scraped users item change to db
    # for user in vinted_data['users']:
    #     for item in user['items']:
            # print(json.dumps(item, indent=4, default=str, ensure_ascii=False))
            # vintrackerDatabase.get_scraped_item_data_difference(item, connection)

    # # get all items that exist in scraped users
    # for user in vinted_data['users']:
    #     for item in user['items']:
    #         item_data = vintrackerDatabase.get_item_data(item['item_id'], connection)
    #         print(item_data)


    ## from from vinted-tracker.tasks import add
    # add.delay(2, 2)



    connection.close()
    engine.dispose()



vintrackerServer()

#docker-compose -f vinted-tracker/Docker/docker-compose.yml up -d --force-recreate &&  celery multi restart w1 -A vinted-tracker -l INFO


