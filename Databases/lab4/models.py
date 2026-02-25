import datetime
import enum
from sqlalchemy import Boolean, Column, Date, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from database import Base


class Performance(Base):
    __tablename__ = 'performances'

    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    genre = Column(String(50), nullable=False)
    duration = Column(Integer, nullable=False)  # minutes
    premier_date = Column(Date)
    base_price = Column(Float, nullable=False)
    producer = Column(String(50), nullable=False)
    age_restriction = Column(Integer, nullable=False)
    is_scheduled = Column(Boolean, default=True)

    tickets = relationship('Ticket', back_populates='performance')
    roles = relationship('Role', back_populates='performance')


class Spectator(Base):
    __tablename__ = 'spectators'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)
    phone = Column(String(25), nullable=False)
    last_visit_date = Column(Date)
    registration_date = Column(Date, default=datetime.date.today())
    visits_count = Column(Integer, default=0)  

    tickets = relationship('Ticket', back_populates='spectator') 


class Actor(Base):
    __tablename__ = 'actors'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    specialization = Column(String(50), nullable=False)
    months_exp = Column(Integer, nullable=False)
    birthday = Column(Date, nullable=False)
    salary = Column(Float, nullable=False)
    job_date = Column(Date, nullable=False)

    roles = relationship('Role', back_populates='actor')


class Role(Base):
    __tablename__ = 'roles'

    id = Column(Integer, primary_key=True)
    performance_id = Column(Integer, ForeignKey('performances.id'), nullable=False)
    actor_id = Column(Integer, ForeignKey('actors.id'), nullable=False)
    name = Column(String(50), nullable=False)
    value = Column(Float, nullable=False)  # 0..1

    performance = relationship('Performance', back_populates='roles')
    actor = relationship('Actor', back_populates='roles')


class PaymentType(enum.Enum):
    CASH = 'cash'
    CARD = 'card'


class Ticket(Base):
    __tablename__ = 'tickets'
    
    id = Column(Integer, primary_key=True)
    spectator_id = Column(Integer, ForeignKey('spectators.id'), nullable=False)
    performance_id = Column(Integer, ForeignKey('performances.id'), nullable=False)
    seat_num = Column(Integer, nullable=False)
    row = Column(Integer, nullable=False)
    sector = Column(Integer, default=1)
    pay_date = Column(Date, default=datetime.date.today())
    price = Column(Float, nullable=False)
    is_used = Column(Boolean, default=False)
    payment_type = Column(Enum(PaymentType), default=PaymentType.CARD)

    performance = relationship('Performance', back_populates='tickets')
    spectator = relationship('Spectator', back_populates='tickets')
