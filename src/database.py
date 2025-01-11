from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Float, String, Integer, DateTime

# Create the SQLAlchemy base class (on version 2.x)
Base = declarative_base()

class WeatherCapitals(Base):
    '''
    Defines the database table parameters
    '''
    __tablename__ = 'weather_capitals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(50), nullable=False)
    temperature = Column(Float, nullable=False)
    feels_like_temp = Column(Float, nullable=False)
    humidity = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    description = Column(String(100), nullable=False)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)