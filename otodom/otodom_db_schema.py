from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class OtodomOffer(Base):
    __tablename__ = 'otodom_offer'

    id = Column(Integer, primary_key=True, autoincrement=True)
    website_address = Column(String)
    parking_space = Column(String)
    heating = Column(String)
    additional_information = Column(String)
    building_material = Column(String)
    type_of_construction = Column(String)
    rent = Column(String)
    price = Column(Float)
    area = Column(Float)
    finishing_standard = Column(String)
    security = Column(String)
    balcony_garden_terrace = Column(String)
    windows = Column(String)
    year_of_construction = Column(Integer)
    market = Column(String)
    number_of_rooms = Column(Integer)
    elevator = Column(String)
    ownership_form = Column(String)
    equipment = Column(String)
    utilities = Column(String)
    advertiser_type = Column(String)
    price_per_square_meter = Column(Float)
    floor = Column(String)
    address = Column(String)
    last_visited = Column(DateTime)
    date_removed = Column(DateTime)
    date_added = Column(DateTime)

    otodom_price_history = relationship('OtodomPriceHistory', back_populates='otodom_offer')
    otodom_photos = relationship('OtodomPhotos', back_populates='otodom_offer')


class OtodomPhotos(Base):
    __tablename__ = 'otodom_photos'

    id = Column(Integer, primary_key=True, autoincrement=True)
    otodom_offer_id = Column(Integer, ForeignKey('otodom_offer.id'))
    path = Column(String)
    original_web_address = Column(String)

    otodom_offer = relationship('OtodomOffer', back_populates='otodom_photos')


class OtodomPriceHistory(Base):
    __tablename__ = 'otodom_price_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    otodom_offer_id = Column(Integer, ForeignKey('otodom_offer.id'))
    price = Column(Float)
    date = Column(DateTime)
    otodom_offer = relationship('OtodomOffer', back_populates='otodom_price_history')


class OtodomScheduledJobs(Base):
    __tablename__ = 'otodom_scheduled_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    frequency = Column(String)
    last_run = Column(DateTime)

    otodom_jobs_history = relationship('OtodomJobsHistory', back_populates='job')


class OtodomJobsHistory(Base):
    __tablename__ = 'otodom_jobs_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    otodom_scheduled_jobs_id = Column(Integer, ForeignKey('otodom_scheduled_jobs.id'))
    rows_inserted = Column(Integer)
    run_date = Column(DateTime)

    job = relationship('OtodomScheduledJobs', back_populates='otodom_jobs_history')
