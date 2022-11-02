from flask import Flask
from sqlalchemy import create_engine, MetaData, insert, inspect
from sqlalchemy import Table, Column, Integer, String, Float, Boolean, Date
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

import pandas as pd


# engine = create_engine('[DB_TYPE]+[DB_CONNECTOR]://[USERNAME]:[PASSWORD]@[HOST]:[PORT]/[DB_NAME]')
engine = create_engine(
    'postgresql://fusemachines:hello123@localhost:5432/postgres')

app = Flask(__name__)

Base = declarative_base()  # create a base class for our class definitions

# create a session object to connect to the DB
session = sessionmaker(bind=engine)()

############################################# MODELS #########################################################


class Crimes(Base):
    __tablename__ = 'crimes'
    id = Column(Integer, primary_key=True)
    incident_number = Column(String(20))
    offense_code = Column(Integer, ForeignKey('offense_codes.offense_code'))
    offense_code_group = Column(String(50))
    offense_description = Column(String(100))
    district = Column(String(5), ForeignKey(
        'police_district_codes.district_code'))
    reporting_area = Column(String(5))
    shooting = Column(String(1))
    occurred_on_date = Column(Date)
    year = Column(Integer)
    month = Column(Integer)
    day_of_week = Column(String(10))
    hour = Column(Integer)
    ucr_part = Column(String(25))
    street = Column(String(50))
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(String(30))

    offense_code = relationship("Offense_Codes", back_populates="crimes")
    district = relationship("Police_District_Codes", back_populates="crimes")


class Offense_Codes(Base):
    __tablename__ = 'offense_codes'
    offense_code = Column(Integer, primary_key=True)
    offense_name = Column(String(50))

    crime = relationship("Crimes", back_populates="offense_codes")


class Police_District_Codes(Base):
    __tablename__ = 'police_district_codes'
    district_code = Column(String(5), primary_key=True)
    district_name = Column(String(50))

    crime = relationship("Crimes", back_populates="police_district_codes")


# function to create all of the above tables in the metadata
def create_tables():
    Base.metadata.create_all(engine)


# function to drop all of the above tables in the metadata
def drop_tables():
    Base.metadata.drop_all(engine)


# function to insert(csv) data into the tables

def insert_data():
    crimes_df = pd.read_csv('/home/saurav/Downloads/crime.csv', encoding='windows-1252')
    offense_codes_df = pd.read_csv(
        '/home/saurav/Downloads/offense_codes.csv', encoding='windows-1252')
    police_district_codes_df = pd.read_csv('/home/saurav/Downloads/police_district_code.csv', encoding='windows-1252')

    # fill empty SHOOTING column with 'N'
    crimes_df['SHOOTING'].fillna('N', inplace=True)

    # convert OCCURRED_ON_DATE to datetime
    crimes_df['OCCURRED_ON_DATE'] = pd.to_datetime(
        crimes_df['OCCURRED_ON_DATE'])

    # keep only first duplicate value in offence_code_df
    offense_codes_df.drop_duplicates(subset='CODE', keep='first', inplace=True)

    # # save to Pre_processed_DATA folder
    # crimes_df.to_csv('Pre_processed_DATA/crimes.csv', index=False)
    # offense_codes_df.to_csv(
    #     'Pre_processed_DATA/offense_codes.csv', index=False)
    # police_district_codes_df.to_csv(
    #     'Pre_processed_DATA/police_district_codes.csv', index=False)

    # insert in order to avoid foreign key constraint error
    crimes_df.to_sql('crimes', engine, if_exists='replace', index=False)
    offense_codes_df.to_sql('offense_codes', engine,
                            if_exists='replace', index=False)

    police_district_codes_df.to_sql('police_district_codes', engine, if_exists='replace', index=False)

    session.commit()


if __name__ == '__main__':

    # be careful with below functions . comment them out when not in use

    create_tables()
    insert_data()  # please comment out this function after first use

    # drop_tables()  # drop all tables

    app.run(debug=True)