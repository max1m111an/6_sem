from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

db_scheme = 'postgresql+psycopg2'
db_user = 'postgres'
db_pass = '191919'
db_host = 'localhost'
db_name = 'theatre'

engine = create_engine(
    f'{db_scheme}://{db_user}:{db_pass}@{db_host}/{db_name}',
)

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
