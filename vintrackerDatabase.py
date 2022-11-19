from sqlalchemy import create_engine, insert, MetaData, Table, Integer, BigInteger, String, Float, Column, select, DateTime, Boolean, ForeignKey, Numeric, CheckConstraint, TIMESTAMP
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import psycopg2
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


def get_database_engine(username, password, ip, database, loglevel):

    try:
        engine = create_engine(
            "postgresql+psycopg2://" +
            username +
            ":" +
            password +
            "@" +
            ip +
            "/" +
            database,
            echo=loglevel)
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

                    Column('user_id', BigInteger, nullable=False, primary_key=True),
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
                    Column('moderator', Boolean),
                    )

    user_data_change = Table('user_data_change', metadata,

                    Column('date_time_scraped', DateTime, nullable=False, index=True),

                    Column('user_id', BigInteger, nullable=False, primary_key=True),
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

                    Column('user_id', BigInteger, nullable=False),


                    Column('item_id', BigInteger, nullable=False, primary_key=True),
                    Column('price', Numeric),
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
                    Column('favourites', Integer),
                    Column('view_count', Integer),
                    Column('is_admin_alerted', Boolean),
                    Column('active_bid_count', Integer),
                    )

    item_data_change = Table('item_data_change', metadata,

                    Column('initially_scraped', DateTime, nullable=False, index=True),

                    Column('item_id', BigInteger, nullable=False),
                    Column('price', Numeric),
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
                    Column('favourites', Integer),
                    Column('view_count', Integer),
                    Column('is_admin_alerted', Boolean),
                    Column('active_bid_count', Integer),
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
# TODO: insert_user_data insert only necessary changed values, make dynamic query
def insert_user_data(user_data, connection):
    # need to save copy for later re adding to user_data
    item_data = user_data['items']
    # print(json.dumps(user, default=str))
    # print(user_data.keys())

    # dont want user items in db user_data table
    user_data.pop('items')

    insert_item_attribute_statements = str()
    insert_item_value_statements = str()
    counter = 0


    attribute_count = len(user_data.keys())

    # build dynamic query
    for item_attribute in user_data.keys():
        if counter < (attribute_count - 1):
            insert_item_attribute_statements += item_attribute + ","
            insert_item_value_statements += ":" + item_attribute + ","
        else:
            insert_item_attribute_statements += item_attribute + ")"
            insert_item_value_statements += ":" + item_attribute + ")"
        counter += 1

    query = text('INSERT INTO user_data(' + insert_item_attribute_statements + ' VALUES(' +  insert_item_value_statements  + 'ON CONFLICT (item_id) DO NOTHING')
    # print(query)
    # print(json.dumps(user_data, indent=4, sort_keys=False, default=str, ensure_ascii=False))
    # # query = text("""INSERT INTO user_data(initially_scraped,user_id,user_login,following_count,followers_count,total_items_count,avg_response_time,volunteer_moderator,closet_promoted_until,profile_url,is_online,has_promoted_closet,about,feedback_reputation,is_shadow_banned,negative_feedback_count,country_iso_code,city_id,city_name,country_title_local,country_title,is_hated) VALUES(:initially_scraped,:user_id,:user_login,:following_count,:followers_count,:total_items_count,:avg_response_time,:volunteer_moderator,:closet_promoted_until,:profile_url,:is_online,:has_promoted_closet,:about,:feedback_reputation,:is_shadow_banned,:negative_feedback_count,:country_iso_code,:city_id,:city_name,:country_title_local,:country_title,:is_hated)""")
    try:
        id = connection.execute(query, user_data)
        print("Rows Added from " + user_data['user_login'] + " = ", id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

    user_data.update(({"items": item_data}))
# passing item dict so use .keys() 13.03
def insert_item_data(item_data, connection):

    # set timestamp
    # item.update([("initially_scraped", datetime.now())])

    # print(type(item_data))
    # print(json.dumps(item_data, indent=4, sort_keys=False, default=str, ensure_ascii=False))
    # print(item_data.keys())

    insert_item_attribute_statements = str()
    insert_item_value_statements = str()
    counter = 0
    attribute_count = len(item_data.keys())
    item_attributes = item_data.keys()

    for item_attribute in item_attributes:
        if counter < (attribute_count - 1):
            insert_item_attribute_statements += item_attribute + ","
            insert_item_value_statements += ":" + item_attribute + ","
        else:
            insert_item_attribute_statements += item_attribute + ")"
            insert_item_value_statements += ":" + item_attribute + ")"
        counter += 1


    query = text('INSERT INTO item_data(' + insert_item_attribute_statements + ' VALUES(' +  insert_item_value_statements + 'ON CONFLICT (item_id) DO NOTHING')
    print(query)

    try:

        id = connection.execute(query, item_data)
        print("Rows Added from " + str(item_data['user_id']) + " = ", id.rowcount)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

# TODO find out how to update whole row on conflict postgresql or else wont have latest item state in DB, maybe can hash scraped_vinted_data to detect change insrtead of checking each row itself
def update_item_data(item_data, connection):

    # set timestamp
    # item.update([("initially_scraped", datetime.now())])

    # print(type(item_data))
    # print(json.dumps(item_data, indent=4, sort_keys=False, default=str, ensure_ascii=False))
    # print(item_data.keys())

    insert_item_attribute_statements = str()
    insert_item_value_statements = str()
    counter = 0
    attribute_count = len(item_data.keys())
    item_attributes = item_data.keys()

    for item_attribute in item_attributes:
        if counter < (attribute_count - 1):
            insert_item_attribute_statements += item_attribute + ","
            insert_item_value_statements += ":" + item_attribute + ","
        else:
            insert_item_attribute_statements += item_attribute + ")"
            insert_item_value_statements += ":" + item_attribute + ")"
        counter += 1


    query = text('UPDATE item_data(' + insert_item_attribute_statements + ' VALUES(' +  insert_item_value_statements + 'ON CONFLICT (item_id) DO NOTHING')
    print(query)

    try:

        id = connection.execute(query, item_data)
        print("Rows Added from " + str(item_data['user_id']) + " = ", id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

def insert_item_change(item_data, connection):
    print(item_data)

    # set timestamp
    # item.update([("initially_scraped", datetime.now())])
    insert_item_attribute_statements = str()
    insert_item_value_statements = str()
    counter = 0
    attribute_count = len(item_data.keys())
    # build dynamic query
    for item_attribute in item_data.keys():
        if counter < (attribute_count - 1):
            insert_item_attribute_statements += item_attribute + ","
            insert_item_value_statements += ":" + item_attribute + ","
        else:
            insert_item_attribute_statements += item_attribute + ")"
            insert_item_value_statements += ":" + item_attribute + ")"
        counter += 1

    query = text('INSERT INTO item_data_change(' + insert_item_attribute_statements + ' VALUES(' +  insert_item_value_statements + 'ON CONFLICT (item_id) DO NOTHING' )

    try:
        print(query)
        id = connection.execute(query, item_data)
        print("Rows Added from ItemID " + str(item_data['item_id']) + " = ", id.rowcount)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

def insert_user_change(user_change, connection):
    print(user_change)

def get_change_user_data(start, end, user_id):
    return True

def get_change_item_data(start, end, item_id):
    return True

def get_user_data(user_id, connection):

    query = text(
        "SELECT * FROM user_data WHERE user_id='" +
        str(user_id) +
        "'")
    row_as_dict = dict()

    try:
        # print(item)
        query_results = connection.execute(query)

        for row in query_results:
            row_as_dict = dict(row)

        row_as_dict['initially_scraped'] = str(row_as_dict['initially_scraped'])
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

    # print(json.dumps(row_as_dict, indent=4, sort_keys=False, default=str, ensure_ascii=False))

    if len(row_as_dict) == 0:
        return "Error: Item not found!"
    else:
        return row_as_dict

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

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)

    # print(json.dumps(row_as_dict, indent=4, sort_keys=False, default=str, ensure_ascii=False))

    if len(row_as_dict) == 0:
        return "Error: Item not found!"
    else:
        return row_as_dict

def get_scraped_user_data_difference(scraped_users, connection):

    # add unchanged or new items here
    users_to_pop = list()


    for user in scraped_users:
        # remove for this function irrelevant items list
        # print(json.dumps(user, default=str))
        user.pop('items')
        # get user from database
        db_user = dict()
        db_user = get_user_data(user['user_id'], connection)
        # if user doesnt exist in database skip to next user
        if isinstance(db_user, str):
            print(
                "\nuser is currently not in database! inserting into db and popping user from scraped users\n")
            insert_user_data(user, connection)
            users_to_pop.append(scraped_users.index(user))
            continue

        # save original scrape date and user_id, as it gets deleted for later
        # comparsion and re-added when inserting changed data to
        # db.user_data_change
        date_scraped = user['initially_scraped']
        user_id = db_user['user_id']
        # user_id = db_user['user_id']

        # TEST DATA!!!
        # user['title'] = "fuckity fuck"
        # db_user['favourites'] = 65
        # print(str(user['user_id']) + " view count: " + str(user['view_count']) + " and in db: " + str(db_user["view_count"]))

        # harmonize data sets
        db_user.pop('initially_scraped')
        user.pop('initially_scraped')

        # print(json.dumps(db_user, indent=4))
        # print(json.dumps(user, indent=4))

        unchanged_user_attributes = list()
        if db_user == user:
            # print("\nno change! pop '" + user['title'] + "' from scraped_users\n")
            users_to_pop.append(scraped_users.index(user))
            continue
        else:
            # print("\nsomething changed!\n")
            for user_attribute in user:
                # find unchanged users
                if user[user_attribute] == db_user[user_attribute]:
                    unchanged_user_attributes.append(user_attribute)
                else:
                    if user_attribute in arithmetic_attributes:
                        # print(user_attribute + str(type(user[user_attribute])) + "-" +  str(type(db_user[user_attribute])))
                        scraped_users[scraped_users.index(
                            user)][user_attribute] = user[user_attribute] - db_user[user_attribute]

        # pop unchanged user_attributes from changed user (minimize database
        # size/redundancy this way )
        for unchanged_attribute in unchanged_user_attributes:
            # user.pop(unchanged_attribute)
            scraped_users[scraped_users.index(user)].pop(unchanged_attribute)

        # re-append date_scraped, user_id for insertion to db as these shouldnt be non existent
        scraped_users[scraped_users.index(user)].update(
            ({"initially_scraped": date_scraped, "user_id": user_id}))

    # print(len(scraped_users))
    # print(users_to_pop)

    # pop unchanged users or new users from scraped_users, need to pop in
    # reverse as index values change dynamically as users get popped
    for user in sorted(users_to_pop, reverse=True):
        # print(user)
        # print(scraped_users[user])
        scraped_users.pop(user)

    # print(json.dumps(scraped_users, default=str, indent=4))

        # print(60.0-65.0)
        # print(type(user))
    # print(len(scraped_users))
    return scraped_users
# returns items and its values that need to be updated/inserted into the database
def get_scraped_item_data_difference(scraped_items, connection):

    # add unchanged or new items here
    items_to_pop = list()

    for item in scraped_items:
        # print(item)
        # get item from database
        db_item = dict()
        db_item = get_item_data(item['item_id'], connection)
        # print(db_item)
        # if item doesnt exist in database skip to next item
        if isinstance(db_item, str):
            print(
                "\nitem is currently not in database!adding items and popping item from scraped items and continuing\n")
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
                            item)][item_attribute] = db_item[item_attribute] - item[item_attribute]

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
    try:
        f = open(os.path.dirname(os.path.realpath(__file__)) +
             '/Config/vinted_database_config.json')
        print("Read local config file")
    except OSError as oe:
        print(oe)

    # Reading from file
    config = json.loads(f.read())

    try:
        # postgre engine instantiation
        engine = get_database_engine(
            config['pg_server_admin'],
            config['pg_server_admin_password'],
            config['pg_server_ip'],
            "vintracker_database",
            False)
        print("Got DB Engine Object")

        # # vintracker first time database setup
        create_default_database(engine)
        print("Created default DB")

        create_default_tables(engine)
        print("Created default DB Tables")

        create_default_users(
            config['vintracker_admin'],
            config['vintracker_admin_pw'],
            config['vintracker_scraper'],
            config['vintracker_scraper_pw'],
            engine,
            False)
        print("Created default DB Users")
    except SQLAlchemyError as e:
        print(type(e))

    print("Finished DB Setup!")

def debug_main():

    # # read local config file
    f = open(os.path.dirname(os.path.realpath(__file__)) +
             '/Config/vinted_database_config.json')

    # # reading config from file
    config = json.loads(f.read())

    # # create database engine
    engine = get_database_engine(
        config['pg_server_admin'],
        config['pg_server_admin_password'],
        config['pg_server_ip'],
        "vintracker_database",
        False)

    # # vintracker first time database setup
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

    # # user data to be scraped
    user_ids = [52446954, 54756595, 44787336]

    vinted_data = vintrackerScraper.scrape_user(user_ids)

    # # insert users to db
    # for user in vinted_data['users']:
        # print( user  )
        # insert_user_data(user, connection)


    # # insert every scraped users items to db
    # for user in vinted_data['users']:
    #     for item in user['items']:
    #         # print(json.dumps(item, indent=4, default=str, ensure_ascii=False))
    #         insert_item_data(item, connection)


    # # get all items that exist in scraped users
    # for user in vinted_data['users']:
    #     for item in user['items']:
    #         # item_data = get_item_data(item['item_id'], connection)
    #         print(item_data)


    # # get item change
    # item_change = dict()
    # for user in vinted_data['users']:
    #     item_change = get_scraped_item_data_difference(user['items'], connection)
    #     db_item = get_item_data(item_change[0]["item_id"],connection)


    # # get user change
    user_change = dict()
    user_change = get_scraped_user_data_difference(vinted_data['users'], connection)
    db_item = get_user_data(user_change[0]["user_id"],connection)

    # print(json.dumps(user_change, indent=4, ensure_ascii=False))

    # get user data
    # print(json.dumps(get_user_data(user_ids[0], connection), indent=4, ensure_ascii=False))

    connection.close()
    engine.dispose()


