from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session

DATABASE_URL = 'postgresql://postgres:02589qwert;!/A@localhost:5432/postgres'
engine = create_engine(DATABASE_URL)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()


@contextmanager
def create_session():
    session = Session
    try:
        yield session
        session.commit()
    finally:
        session.close()


class Offer(Base):
    __tablename__ = 'offer'

    id = Column(Integer, primary_key=True)
    offer_id = Column(String)
    website_address = Column(String)
    city = Column(String)
    district = Column(String, index=True)
    price = Column(Float)
    area = Column(Float)
    price_per_square_meter = Column(Float)
    floor = Column(String)
    number_of_rooms = Column(Integer)
    building_type = Column(String)
    ownership_type = Column(String)
    year_of_construction = Column(Integer)
    date_added = Column(DateTime)
    date_removed = Column(DateTime)
    last_visited = Column(DateTime)

    price_history = relationship('PriceHistory', back_populates='offer')
    photos = relationship('Photo', back_populates='offer')


class PriceHistory(Base):
    __tablename__ = 'price_history'

    id = Column(Integer, primary_key=True)
    offer_id = Column(Integer, ForeignKey('offer.id'))
    price = Column(Float)
    date = Column(DateTime)

    offer = relationship('Offer', back_populates='price_history')


class Photo(Base):
    __tablename__ = 'photos'

    id = Column(Integer, primary_key=True)
    offer_id = Column(Integer, ForeignKey('offer.id'))
    path = Column(String)
    original_web_address = Column(String)

    offer = relationship('Offer', back_populates='photos')


class ScheduledJob(Base):
    __tablename__ = 'scheduled_jobs'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    frequency = Column(String)
    last_run = Column(DateTime)

    jobs_history = relationship('JobHistory', back_populates='job')


class JobHistory(Base):
    __tablename__ = 'jobs_history'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('scheduled_jobs.id'))
    run_date = Column(DateTime)
    rows_inserted = Column(Integer)

    job = relationship('ScheduledJob', back_populates='jobs_history')


# Create tables in the database
Base.metadata.create_all(bind=engine)


class LoggedError(Base):
    __tablename__ = 'logged_errors'

    id = Column(Integer, primary_key=True, autoincrement=True)
    process_name = Column(String)
    value = Column(String)
    date = Column(DateTime)
    error_message = Column(String(98000))



