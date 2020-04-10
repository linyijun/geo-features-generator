from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


connection_string = 'postgresql+psycopg2://{usr}:{pwd}@jonsnow.usc.edu/air_quality_dev' \
    .format(usr='yijun', pwd='m\\tC7;cc')

engine = create_engine(connection_string, echo=False)

Session = sessionmaker(bind=engine, expire_on_commit=False)

Base = declarative_base()

session = Session()
