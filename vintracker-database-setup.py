from sqlalchemy import create_engine, insert, MetaData, Table, Integer, String, Column, select, DateTime, ForeignKey, Numeric, CheckConstraint
from sqlalchemy_utils import database_exists, create_database
import os
import json

def get_database_engine(username, password, ip, database):
    engine = create_engine("postgres+psycopg2://" + username + ":" + password + "@" + ip + "/" + database, echo=True)
    return engine


def create_default_database(engine):
    if not database_exists(engine.url):
        create_database(engine.url)
    return True

# TODO finish table schemas; find out how to depict 1 - n relationship e.g. login_data.id should be related to many item_data.user_id
def create_default_tables(engine):
    metadata = MetaData()

    login_data = Table('login_data', metadata,
    Column('id', String(50), nullable=False),
    Column('username', String(50), nullable=False),
    Column('hashed_password', String(200), nullable=False),
    )

    tracking_data = Table('tracking_data', metadata,
    Column('id', String(50), nullable=False),
    Column('user_id', int(15), nullable=False),
    )
   
    item_data = Table('item_data', metadata,
    Column('user_id', int(15), nullable=False),
    Column('article_name', String(100), nullable=False),
    Column('article_description', String(500), nullable=False),
    )
    
    metadata.create_all(engine)

    return True

# TODO finish default user: vintracker_admin vintracker_scraper which should be able to update, insert, delete scraped data to database
def create_default_users(vintracker_admin, vintracker_admin_pw, vintracker_scraper, vintracker_scraper_pw, engine):
    create_user_statement = "CREATE USER " + vintracker_admin +  " WITH PASSWORD '" + vintracker_admin_pw +"';"
    print(create_user_statement)
    return True

def main():

    # read local config file 
    f = open (os.path.dirname(os.path.realpath(__file__)) + '\\config\\vinted_database_config.json')
    
    # Reading from file 
    config = json.loads(f.read())
    
    # postgre engine instantiation
    engine = get_database_engine(config['pg_server_admin'], config['pg_server_admin_password'], config['pg_server_ip'], "vintracker_database")

    # # vintracker first time database setup
    create_default_database(engine)
    create_default_tables(engine)
    create_default_users(config['vintracker_admin'], config['vintracker_admin_pw'], config['vintracker_scraper'], config['vintracker_scraper_pw'], engine)







main()


