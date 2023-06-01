from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, DateTime
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


# define the database engine to connect to
DB_ENGINE = 'postgresql://postgres:rQdPQKJjiboCsU28bQtJ@containers-us-west-91.railway.app:7252/railway'
# define the base class for the ORM
Base= declarative_base()

class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False) #Worker or Admin    


if __name__ == '__main__':
   # connect to the database engine
   engine= create_engine(DB_ENGINE, echo=True, future=True)
   engine.connect()

   Base.metadata.create_all(engine)

   with Session(engine) as session:

       #crete users and add them to the table 'Users'
       new_user1 = Users(name="Asen", role="Admin")
       new_user2 = Users(name="Jose", role="Worker")

       session.add_all([new_user1, new_user2])

       session.commit()
   
