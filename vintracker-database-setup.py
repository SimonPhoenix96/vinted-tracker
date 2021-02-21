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

# TODO finish table schemas; find out how to depict 1 - n relationship e.g. login_data.id should be related to many item_data.user_id
def create_default_tables(engine):
    metadata = MetaData()

    login_data = Table('vintracker_data', metadata,
    Column('username', Integer, nullable=False, primary_key=True),
    Column('hashed_password', String(200), nullable=False),
    Column('config', String)

    )

    user_data = Table('user_data', metadata,
    Column('user_id', Integer, nullable=False, primary_key=True),
    Column('username', Integer, nullable=False),
    Column('followers', Integer, nullable=False),
    Column('following', Integer, nullable=False),
    Column('total_items', Integer, nullable=False),
    Column('avg_response_time', Integer, nullable=False),
    Column('closet_promoted_until', Integer, nullable=False),
    Column('profile_url', Integer, nullable=False),
    Column('date_scraped', String(1000), nullable=False),

    )

    item_data = Table('item_data', metadata,
    Column('item_id', Integer, nullable=False, primary_key=True),
    Column('user_id', Integer, nullable=False),
    Column('url', String(100)),
    Column('title', String(500)),
    Column('price', String(25)),
    Column('brand', String(100)),
    Column('label', String(100)),
    Column('size', String(100)),
    Column('favourites', String(100)),
    Column('view_count', String(100)),
    Column('active_bid_count', String(100)),
    Column('photos', String(5000)),
    Column('date_scraped', String(1000), nullable=False),
    
    )
    
    # probably not needed
    # tracking_data = Table('tracking_data', metadata,
    # Column('username',Integer, nullable=False, primary_key=True),
    # Column('tracked_user_id', Integer, ForeignKey("item_data.id")),
    # )
    
    
    

    metadata.create_all(engine)


# TODO finish default user: vintracker_admin vintracker_scraper which should be able to update, insert, delete scraped data to database
def create_default_users(vintracker_admin, vintracker_admin_pw, vintracker_scraper, vintracker_scraper_pw, engine):
    con = engine.connect()
    create_user_statements = ({  "CREATE USER " + vintracker_admin +  " WITH PASSWORD '" + vintracker_admin_pw +"';"},
                                "CREATE USER " + vintracker_scraper +  " WITH PASSWORD '" + vintracker_scraper_pw +"';"
                            )
    for line in create_user_statements:
        con.execute(create_user_statements, **line)
    # print(create_user_statement)

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
    # create_default_users(config['vintracker_admin'], config['vintracker_admin_pw'], config['vintracker_scraper'], config['vintracker_scraper_pw'], engine)







main()


