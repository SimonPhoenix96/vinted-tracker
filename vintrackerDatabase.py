from sqlalchemy import create_engine, insert, MetaData, Table, Integer, String, Float, Column, select, DateTime, Boolean, ForeignKey, Numeric, CheckConstraint, TIMESTAMP
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import os
import time
import json
import decimal
from datetime import datetime
from itertools import chain

import random

# Vintracker Modules
import vintrackerScraper

# global variables

arithmetic_attributes = (
    'active_bid_count',
    'view_count',
    'favourites',
    'price'
    )

string_attributes = (
    'title',
    'currency_symbol',
    'description',
    'brand',
    'size',
    'status',
    'label',
    'gender',
    'color1',
    'catalog_id',
    'url',
    'reserved_for_user_id',
    'is_admin_alerted',
    'photos'
    )


def get_database_engine(username, password, ip, database, debug):

    try:
        engine = create_engine(
            "postgres+psycopg2://" +
            username +
            ":" +
            password +
            "@" +
            ip +
            "/" +
            database,
            echo=debug)
        return engine

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)


def create_default_database(engine):

    try:
        if not database_exists(engine.url):
            create_database(engine.url)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)


def create_default_tables(engine):

    metadata = MetaData()

    login_data = Table('vintracker_data', metadata, 
                    Column('username', Integer, nullable=False, primary_key=True), 
                    Column('hashed_password', String(200), nullable=False), 
                    Column('config', String)
                    )

    user_data = Table('user_data', metadata,

                    Column('initially_scraped', DateTime(timezone=True), nullable=False, index=True),

                    Column('user_id', Integer, nullable=False, primary_key=True),
                    Column('user_login', String(50), nullable=False),

                    Column('following_count', Integer),
                    Column('followers_count', Integer),
                    Column('total_items_count', Integer),
                    Column('avg_response_time', Integer),
                    Column('volunteer_moderator', Boolean),
                    Column('closet_promoted_until', Integer),
                    Column('profile_url', String(1000)),
                    Column('is_online', Boolean),
                    Column('has_promoted_closet', Boolean),
                    Column('about', String(1000)),
                    Column('feedback_reputation', Float),
                    Column('is_shadow_banned', Boolean),
                    Column('negative_feedback_count', Integer),
                    Column('country_iso_code', String(5)),
                    Column('city_id', String(20)),
                    Column('city_name', String(20)),
                    Column('country_title_local', String(20)),
                    Column('country_title', String(20)),
                    Column('is_hated', Boolean),
                    )

    user_data_change = Table('user_data_change', metadata,

                    Column('date_time_scraped', DateTime, nullable=False, index=True),

                    Column('user_id', Integer, nullable=False, primary_key=True),
                    Column('user_login', String(50), nullable=False),

                    Column('following_count', Integer),
                    Column('followers_count', Integer),
                    Column('total_items_count', Integer),
                    Column('avg_response_time', Integer),
                    Column('volunteer_moderator', Boolean),
                    Column('closet_promoted_until', Integer),
                    Column('profile_url', String(1000)),
                    Column('is_online', Boolean),
                    Column('has_promoted_closet', Boolean),
                    Column('about', String(1000)),
                    Column('feedback_reputation', Float),
                    Column('is_shadow_banned', Boolean),
                    Column('negative_feedback_count', Integer),
                    Column('country_iso_code', String(5)),
                    Column('city_id', String(20)),
                    Column('city_name', String(20)),
                    Column('country_title_local', String(20)),
                    Column('country_title', String(20)),
                    Column('is_hated', Boolean),
                    )

    item_data = Table('item_data', metadata,

                    Column('initially_scraped', DateTime(timezone=True), nullable=False, index=True),

                    Column('user_id', Integer, nullable=False),


                    Column('item_id', Integer, nullable=False, primary_key=True),

                    Column('title', String(100)),
                    Column('currency_symbol', String(10)),
                    Column('price', Numeric),
                    Column('description', String(5000)),
                    Column('brand', String(100)),
                    Column('label', String(100)),
                    Column('size', String(25)),
                    Column('gender', String(10)),
                    Column('color1', String(25)),
                    Column('status', String(25)),
                    Column('photos', String(5000)),
                    Column('catalog_id', Integer),
                    Column('url', String(200)),
                    Column('reserved_for_user_id', String(25)),
                    Column('favourites', Integer),
                    Column('view_count', Integer),
                    Column('is_admin_alerted', Boolean),
                    Column('active_bid_count', Integer),
                    )

    item_data_change = Table('item_data_change', metadata, 
        
                    Column('initially_scraped', DateTime, nullable=False, index=True), 
                    Column('user_id', Integer, nullable=False, primary_key=True),
                    Column('item_id', Integer, nullable=False, primary_key=True), 
                    
                    Column('title', String(100)),
                    Column('currency_symbol', String(10)),
                    Column('description', String(5000)),
                    Column('brand', String(100)),
                    Column('label', String(100)),
                    Column('size', String(25)),
                    Column('gender', String(10)),
                    Column('color1', String(25)),
                    Column('status', String(25)),
                    Column('photos', String(5000)),
                    Column('catalog_id', Integer),
                    Column('url', String(200)),
                    Column('reserved_for_user_id', String(25)),
                    Column('is_admin_alerted', Boolean),
                    )
    try:
        metadata.create_all(engine)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)


# TODO finish setting user permissions: vintracker_admin, vintracker_scraper which should be able to query, update, insert scraped data to database
def create_default_users(
        vintracker_admin,
        vintracker_admin_pw,
        vintracker_scraper,
        vintracker_scraper_pw,
        engine,
        debug):
    con = engine.connect()

    create_user_statements = (
        "CREATE USER " +
        vintracker_admin +
        " WITH PASSWORD '" +
        vintracker_admin_pw +
        "';",
        "CREATE USER " +
        vintracker_scraper +
        " WITH PASSWORD '" +
        vintracker_scraper_pw +
        "';")

    try:
        for stmt in create_user_statements:
            con.execute(stmt)
        # print(create_user_statement)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        if debug:
            print(error)


def insert_user_data(user, connection):

    # print(json.dumps(user, default=str))
    # TODO: insert_user_data insert only necessary changed values, make dynamic query
    query = text("""INSERT INTO user_data(initially_scraped,user_id,user_login,following_count,followers_count,total_items_count,avg_response_time,volunteer_moderator,closet_promoted_until,profile_url,is_online,has_promoted_closet,about,feedback_reputation,is_shadow_banned,negative_feedback_count,country_iso_code,city_id,city_name,country_title_local,country_title,is_hated) VALUES(:initially_scraped,:user_id,:user_login,:following_count,:followers_count,:total_items_count,:avg_response_time,:volunteer_moderator,:closet_promoted_until,:profile_url,:is_online,:has_promoted_closet,:about,:feedback_reputation,:is_shadow_banned,:negative_feedback_count,:country_iso_code,:city_id,:city_name,:country_title_local,:country_title,:is_hated)""")

    try:
        id = connection.execute(query, user)
        print("Rows Added  = ", id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

# find way to add photos, prob make column accept array values


def insert_item_data(item, connection):

    # set timestamp
    # item.update([("initially_scraped", datetime.now())])
    # TODO: insert_item_data insert only necessary changed values, make dynamic query
    query = text("""INSERT INTO item_data(initially_scraped,user_id,item_id,title,currency_symbol,price,description,brand,label,size,gender,color1,status,catalog_id,url,reserved_for_user_id,favourites,view_count,is_admin_alerted,active_bid_count) VALUES(:initially_scraped,:user_id,:item_id,:title,:currency_symbol,:price,:description,:brand,:label,:size,:gender,:color1,:status,:catalog_id,:url,:reserved_for_user_id,:favourites,:view_count,:is_admin_alerted,:active_bid_count)""")

    try:
        # print(item)
        id = connection.execute(query, item)
        print("Rows Added  = ", id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)


def get_user_data():
    return True


def get_item_data(item_id, connection):

    query = text(
        "SELECT * FROM item_data WHERE item_id='" +
        str(item_id) +
        "'")
    row_as_dict = dict()

    try:
        # print(item)
        query_results = connection.execute(query)

        for row in query_results:
            row_as_dict = dict(row)

        # print("Rows Added  = ",id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

    # print(json.dumps(row_as_dict, indent=4, sort_keys=False, default=str, ensure_ascii=False))

    if len(row_as_dict) == 0:
        return "Error: Item not found!"
    else:
        return row_as_dict


def get_scraped_user_data_data_difference():
    return True


# returns items and its values that need to be updated/inserted into the
# database
def get_scraped_item_data_difference(scraped_items, connection):

    # add unchanged or new items here
    items_to_pop = list()

    for item in scraped_items:
        # get item from database
        db_item = dict()
        db_item = get_item_data(item['item_id'], connection)
        # if item doesnt exist in database skip to next item
        if isinstance(db_item, str):
            print(
                "\nitem is currently not in database! inserting into db and popping item from scraped items\n")
            insert_item_data(item, connection)
            items_to_pop.append(scraped_items.index(item))
            continue

        # save original scrape date and item_id, as it gets deleted for later
        # comparsion and re-added when inserting changed data to
        # db.item_data_change
        date_scraped = item['initially_scraped']
        item_id = db_item['item_id']
        # user_id = db_item['user_id']

        # TEST DATA!!!
        # item['title'] = "fuckity fuck"
        # db_item['favourites'] = 65
        # print(str(item['item_id']) + " view count: " + str(item['view_count']) + " and in db: " + str(db_item["view_count"]))

        # harmonize data sets
        db_item.pop('initially_scraped')
        item.pop('initially_scraped')

        # convert price from string to float
        db_item['price'] = float(db_item['price'])
        item['price'] = float(item['price'])

        # ! removing photos temporarily, because i need to find a viable way to store them in the database !
        db_item.pop('photos')
        item.pop('photos')
        # print(json.dumps(db_item, indent=4))
        # print(json.dumps(item, indent=4))

        unchanged_item_attributes = list()
        if db_item == item:
            # print("\nno change! pop '" + item['title'] + "' from scraped_items\n")
            items_to_pop.append(scraped_items.index(item))
            continue
        else:
            # print("\nsomething changed!\n")
            for item_attribute in item:
                # find unchanged items
                if item[item_attribute] == db_item[item_attribute]:
                    unchanged_item_attributes.append(item_attribute)
                else:
                    if item_attribute in arithmetic_attributes:
                        # print(item_attribute + str(type(item[item_attribute])) + "-" +  str(type(db_item[item_attribute])))
                        scraped_items[scraped_items.index(
                            item)][item_attribute] = item[item_attribute] - db_item[item_attribute]

        # pop unchanged item_attributes from changed item (minimize database
        # size/redundancy this way )
        for unchanged_attribute in unchanged_item_attributes:
            # item.pop(unchanged_attribute)
            scraped_items[scraped_items.index(item)].pop(unchanged_attribute)

        # re-append date_scraped, item_id for insertion to db as these shouldnt be non existent
        scraped_items[scraped_items.index(item)].update(
            ({"initially_scraped": date_scraped, "item_id": item_id}))

    # print(len(scraped_items))
    # print(items_to_pop)

    # pop unchanged items or new items from scraped_items, need to pop in
    # reverse as index values change dynamically as items get popped
    for item in sorted(items_to_pop, reverse=True):
        # print(item)
        # print(scraped_items[item])
        scraped_items.pop(item)

    # print(json.dumps(scraped_items, default=str, indent=4))

        # print(60.0-65.0)
        # print(type(item))
    # print(len(scraped_items))
    return scraped_items


def insert_item_change(item_data, connection):

    # set timestamp
    # item.update([("initially_scraped", datetime.now())])

    # TODO: insert_item_change insert only necessary changed values, make dynamic query

    print(str(item_data[0].keys()))
    # for item_attribute in item_data:
    #     print(item_attribute.keys())
    insert_item_statements = str()
    insert_item_value_statements = str()
    counter = 0
    attribute_count = len(item_data[0].keys())
    # build insert query up to VALUES part of query statement 
    for item_attribute in item_data[0].keys():
        if counter < (attribute_count - 1):
            insert_item_statements += item_attribute + ","
        else:
            insert_item_statements += item_attribute + ")"
        if counter < (attribute_count - 1):
            insert_item_value_statements += ":" + item_attribute + ","  
        else:
            insert_item_value_statements += ":" + item_attribute + ")"    
        counter += 1
    
    print(insert_item_statements)
    print(insert_item_value_statements)
    query = text("""INSERT INTO item_data_change(initially_scraped,user_id,item_id,url,title,price,brand,label,size,favourites,view_count,active_bid_count,photos) VALUES(:initially_scraped,:user_id,:item_id,:url,:title,:price,:brand,:label,:size,:favourites,:view_count,:active_bid_count,:photos)""")

    # try:
    #     # print(item)
    #     id = connection.execute(query, item_data)
        
    #     print("Rows Added  = ", id.rowcount)

    # except SQLAlchemyError as e:
    #     error = str(e.__dict__['orig'])
    #     print(error)

    return True


def update_user_data(data, connection):
    return True


def update_item_data(item, connection):
    # print(json.dumps(item, default=str))

    # query = text("""UPDATE item_data SET favourites = :favourites
    #                 WHERE item_data.item_id = :item_id""")  # , price,description,brand,label,size,gender,color1,status,catalog_id,url,reserved_for_user_id,favourites,view_count,is_admin_alerted,active_bid_count) VALUES(:initially_scraped,:user_id,:item_id,:title,:currency_symbol,:price,:description,:brand,:label,:size,:gender,:color1,:status,:catalog_id,:url,:reserved_for_user_id,:favourites,:view_count,:is_admin_alerted,:active_bid_count)""")
    print("\nitem change before updating:\n" + json.dumps(item, default=str))
    db_item = get_item_data(item["item_id"], connection)
    print("\ndb_item before updating:\n" + json.dumps(db_item, default=str))
    for item_attribute in item.keys():
        if item_attribute in arithmetic_attributes:
            item.update([(item_attribute, item[item_attribute] + db_item[item_attribute])])
        else:
            print("\nitem_attribute not arithmetic\n")
            if item_attribute in string_attributes:
                item.update([(item_attribute, item[item_attribute])])
    print("\nitem_change summed with db item for later updating the db_item:\n" +  json.dumps(item, default=str))
    
    # try:
    #     # id = connection.execute(query, item)
    #     print("Rows Added  = ", id.rowcount)

    # except SQLAlchemyError as e:
    #     error = str(e.__dict__['orig'])
    #     print(error)
    # return True


def delete_user_data(data):
    return True


def delete_item_data(data):
    return True


def get_change_user_data(start, end, data):
    return True


def get_change_item_data(start, end, data):
    return True


# def get_unique_items(items):
#     unique = []

#     for item in items:
#         if item in unique:
#             continue
#         else:
#             unique.append(item)
#     return unique

def main():

    # read local config file
    f = open(os.path.dirname(os.path.realpath(__file__)) +
             '\\config\\vinted_database_config.json')

    # Reading from file
    config = json.loads(f.read())

    # postgre engine instantiation
    engine = get_database_engine(
        config['pg_server_admin'],
        config['pg_server_admin_password'],
        config['pg_server_ip'],
        "vintracker_database")

    # # vintracker first time database setup
    create_default_database(engine)
    create_default_tables(engine)
    create_default_users(
        config['vintracker_admin'],
        config['vintracker_admin_pw'],
        config['vintracker_scraper'],
        config['vintracker_scraper_pw'],
        engine)


def debug_main():

    # read local config file
    f = open(os.path.dirname(os.path.realpath(__file__)) +
             '\\config\\vinted_database_config.json')

    # reading config from file
    config = json.loads(f.read())
    # create database engine
    engine = get_database_engine(
        config['pg_server_admin'],
        config['pg_server_admin_password'],
        config['pg_server_ip'],
        "vintracker_database",
        False)

    # vintracker first time database setup
    create_default_database(engine)
    create_default_tables(engine)
    create_default_users(
        config['vintracker_admin'],
        config['vintracker_admin_pw'],
        config['vintracker_scraper'],
        config['vintracker_scraper_pw'],
        engine,
        False)

    connection = engine.connect()

    # insert user data to database
    user_ids = [52446954]

    vinted_data = vintrackerScraper.scrape_user(user_ids)
    # user_index =
    # user = list()
    # users = vinted_data['users']
    # print( users.get(52446954)  )

    # print( vinted_data['users'][0].get(52446954)  )
    # for user in vinted_data['users']:
    #     # print( user  )
    #     # insert_user_data(user, connection)
    #     insert_user_data(user, connection)

    # print(json.dumps(item_data['users']))
    # print(json.dumps(vinted_data.get("52446954"), default=str))
    # insert_user_data(vinted_data['users'][0], connection)
    # for user in vinted_data['users']:
    #     for item in user['items']:
    #         insert_item_data(item, connection)
    # print(json.dumps(vinted_data['users'][0]['items'], default=str ))

    # print(row_as_dict)
    # row_as_dict.pop('initially_scraped')
    # row_as_dict.pop('photos')
    # row_as_json = json.dumps(row_as_dict, indent=4, sort_keys=False, default=str, ensure_ascii=False)
    # vinted_data['users'][0]['items'][0].pop('photos')
    # vinted_data['users'][0]['items'][0].pop('initially_scraped')
    # print(json.dumps(vinted_data['users'][0]['items'][0], indent=4, sort_keys=False, default=str, ensure_ascii=False))
    # vinted_data_as_json = json.dumps(vinted_data['users'][0]['items'][0]['price'], indent=4, sort_keys=False, default=str)
    # print(vinted_data_as_json)

    # insert all items from scraped users
    # for user in vinted_data['users']:
    #     # print( user  )
    #     # insert_user_data(user, connection)
    #     insert_item_data(user['items'], connection)

    # get all items  that exist in scraped users
    # for user in vinted_data['users']:
    #     for item in user['items']:
    #         # item_data = get_item_data(item['item_id'], connection)
    #         print(item_data)



    item_change = dict()
    for user in vinted_data['users']:
        item_change = get_scraped_item_data_difference(user['items'], connection)
        db_item = get_item_data(item_change[0]["item_id"],connection)
        # item_change["view_count"] = random.randint(1, 500) 
        # print(json.dumps(db_item, indent=4, default=str))
        # print(json.dumps(item_change, indent=4, default=str))
    # print(json.dumps(get_item_data(5345455, connection), indent=4, sort_keys=False, default=str, ensure_ascii=False))
    # print(json.dumps(item_change, indent=4, default=str))
    insert_item_change(item_change,connection)
    # for item in item_change:
    #     db_item = get_item_data(item['item_id'], connection)
    #     # print(db_item['title'] + "change since: " + str(db_item['initially_scraped']) +  "\n")
    #     item.update({'title': db_item['title'], 'change_since': str(
    #         db_item['initially_scraped'])})
        # print(
        #     "\nITEM IN DB: \n" +
        #     json.dumps(
        #         db_item,
        #         indent=4,
        #         sort_keys=False,
        #         default=str,
        #         ensure_ascii=False))
        # print(
        #     "\nITEM CHANGE: \n" +
        #     json.dumps(
        #         item,
        #         indent=4,
        #         default=str,
        #         ensure_ascii=False))
    # update_item_data(item_change[0], connection)
        # db_item = get_item_data(item['item_id'], connection)
        # print(
        #     "\nITEM IN DB AFTER UPDATING: \n" +
        #     json.dumps(
        #         db_item,
        #         indent=4,
        #         default=str,
        #         ensure_ascii=False))

    
    
    
    
    
    connection.close()
    engine.dispose()


debug_main()
