from models import PaymentType
from database import Base, engine
from populate import fill_db
from queries import *


if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    clear_database()
    fill_db()
    analyze_actor_popularity()