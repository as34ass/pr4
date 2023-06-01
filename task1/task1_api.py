import time
import socket
import json
import datetime

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, DateTime, select, desc, asc
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from fastapi import FastAPI


# define the database engine to connect to
DB_ENGINE = 'postgresql://postgres:rQdPQKJjiboCsU28bQtJ@containers-us-west-91.railway.app:7252/railway'

# define the base class for the ORM
Base= declarative_base()


# connect to the database engine 
engine= create_engine(DB_ENGINE, echo=True, future=True)
engine.connect()

# create the FastAPI instance
app = FastAPI()



# define the database schema for the different data types
class OfficeSensor(Base):
    __tablename__ = 'officesensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    lights = Column(Boolean)
    someone = Column(Boolean)


class Warehouse(Base):
    __tablename__ = 'warehouse'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    power = Column(Boolean, nullable=False)
    temperature = Column(Float, nullable=False)
    
    
class BaySensor(Base):
    __tablename__ = 'baysensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    occupied = Column(Boolean, nullable=False)
    bay_id = Column(Integer, nullable=False)


class TransportBay(Base):
    __tablename__ = 'transportbay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    baysensor_id = Column(Integer, ForeignKey('baysensor.id'), nullable=False)
    baysensor = relationship("BaySensor")
    general_datetime = Column(DateTime, nullable=False)
    general_power = Column(Boolean, nullable=False)
    

class MachineSensor(Base):
    __tablename__ = 'machinesensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    machine_id = Column(Integer, nullable=False)
    working = Column(Boolean, nullable=False)
    faulty = Column(Boolean, nullable=False)


class Machinery(Base):
    __tablename__ = 'machinery'
    id = Column(Integer, primary_key=True, autoincrement=True)
    machinesensor_id = Column(Integer, ForeignKey('machinesensor.id'))
    machinesensor = relationship("MachineSensor")
    general_datetime = Column(DateTime, nullable=False)
    general_power = Column(Boolean, nullable=False)


class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False) #Worker or Admin 

# define endpoints

@app.get("/officesensor")
async def get_officesensor_data(user_name: str = "",
                                order: str = "ascendant",
                                init_date: datetime.date = None,
                                end_date: datetime.date = None):

    with Session(engine) as session:
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            statement = select(OfficeSensor)
            
            if init_date is not None:
                statement = statement.filter(OfficeSensor.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(OfficeSensor.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(OfficeSensor.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(OfficeSensor.datetime.desc())
            results = session.execute(statement).scalars().all()
        
            return [r.__dict__ for r in results]

        else:
            return "Unauthorized", 401


# Define the endpoint to retrieve warehouse data
@app.get("/warehouse")
async def get_warehouse_data(user_name: str = "",
                             order: str = "ascendant",
                             init_date: datetime.date = None,
                             end_date: datetime.date = None):

    with Session(engine) as session:
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            
            statement = select(Warehouse)
            
            if init_date is not None:
                statement = statement.filter(Warehouse.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(Warehouse.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(Warehouse.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(Warehouse.datetime.desc())
            results = session.execute(statement).scalars().all()

            return [r.__dict__ for r in results]

        else:
            return "Unauthorized", 401



    
@app.get("/baysensor")
async def get_baysensor_data(user_name: str = "",
                             order: str = "ascendant",
                             init_date: datetime.date = None,
                             end_date: datetime.date = None):
    
    with Session(engine) as session:
        
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            statement = select(BaySensor)
                
            if init_date is not None:
                statement = statement.filter(BaySensor.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(BaySensor.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(BaySensor.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(BaySensor.datetime.desc())
            results = session.execute(statement).scalars().all()

            return [r.__dict__ for r in results]
        else:
            return "Unauthorized", 401
    



@app.get("/baysensor/{bay_id}")
async def get_baysensor_filter(bay_id: int,
                               user_name: str = "",
                               order: str = "ascendant",
                               init_date: datetime.date = None,
                               end_date: datetime.date = None):
    
    with Session(engine) as session:
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data        
        if user.role == "Admin" or user.role == "Worker":
            statement = select(BaySensor)
            
            if init_date is not None:
                statement = statement.filter(BaySensor.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(BaySensor.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(BaySensor.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(BaySensor.datetime.desc())
            results = session.execute(statement).scalars().all()

            return [sensor for sensor in results if sensor.bay_id == bay_id]
        else:
            return "Unauthorized", 401


@app.get("/transportbay")
def get_transportbay_data(user_name: str = "",
                          order: str = "ascendant",
                          init_date: datetime.date = None,
                          end_date: datetime.date = None):
    
    # Open a new session
    
    with Session(engine) as session:
        
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data        
        if user.role == "Admin" or user.role == "Worker":
            statement = select(TransportBay).join(BaySensor, TransportBay.baysensor_id == BaySensor.id)   

            if init_date is not None:
                statement = statement.filter(TransportBay.general_datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(TransportBay.general_datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(TransportBay.general_datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(TransportBay.general_datetime.desc())
            results = session.execute(statement).scalars().all()

            return [r.__dict__ for r in results]

        else:
            return "Unauthorized", 401




@app.get("/machinesensor")
async def get_machinesensor_data(user_name: str = "",
                                 order: str = "ascendant",
                                 init_date: datetime.date = None,
                                 end_date: datetime.date = None):
    
    with Session(engine) as session:
        
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            statement = select(MachineSensor)
            
            if init_date is not None:
                statement = statement.filter(MachineSensor.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(MachineSensor.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(MachineSensor.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(MachineSensor.datetime.desc())
            results = session.execute(statement).scalars().all()

            return [r.__dict__ for r in results]

        else:
            return "Unauthorized", 401



@app.get("/machinesensor/{machine_id}")
async def get_machinesensor_data(machine_id: int,
                                 user_name: str = "",
                                 order: str = "ascendant",
                                 init_date: datetime.date = None,
                                 end_date: datetime.date = None):
    
    with Session(engine) as session:
        
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            statement = select(MachineSensor)
            
            if init_date is not None:
                statement = statement.filter(MachineSensor.datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(MachineSensor.datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(MachineSensor.datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(MachineSensor.datetime.desc())
            results = session.execute(statement).scalars().all()
            return [sensor for sensor in results if sensor.machine_id == machine_id]

        else:
            return "Unauthorized", 401



@app.get("/machinery")
def get_machinery_data(user_name: str = "",
                          order: str = "ascendant",
                          init_date: datetime.date = None,
                          end_date: datetime.date = None):
    
    # Open a new session
    
    with Session(engine) as session:
        # Get user from database by filtering the Users table with user_name 
        user = session.execute(select(Users).filter_by(name = user_name)).scalars().one_or_none()

        # If user not found, return error message with 401 status code
        if user == None:
            return "User not found", 401

        # If user is either an Admin or a Worker, the user can read the data
        if user.role == "Admin" or user.role == "Worker":
            
            statement = select(Machinery).join(MachineSensor, Machinery.machinesensor_id  == MachineSensor.id)   

            if init_date is not None:
                statement = statement.filter(Machinery.general_datetime >= init_date)
            if end_date is not None:
                statement = statement.filter(Machinery.general_datetime <= end_date)
            if order == "ascendant":
                statement = statement.order_by(Machinery.general_datetime.asc())
            elif order == "descendant":
                statement = statement.order_by(Machinery.general_datetime.desc())
            results = session.execute(statement).scalars().all()

            return [r.__dict__ for r in results]  

        else:
            return "Unauthorized", 401
