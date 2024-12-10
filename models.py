# models.py

from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from modules.reflection import reflected_tables  # Import the reflected tables

Base = declarative_base()

class State(Base):
    __table__ = reflected_tables['states']

    # Define primary key explicitly if not auto-detected
    __mapper_args__ = {
        'primary_key': [__table__.c["States/UT's"]]
    }
    
    # Relationships
    # districts = relationship("District", back_populates="state", cascade="all, delete-orphan")
    
    def __repr__(self):
        value = getattr(self, "States/UT's")
        return f"<State(state_ut='{value}')>"

class District(Base):
    __table__ = reflected_tables['districts']

    __mapper_args__ = {
        'primary_key': [__table__.c.id]  # Assuming 'id' is the primary key
    }
    
    # Foreign Key to State
    # state_ut = Column(String, ForeignKey("states.id"))
    
    # Relationships
    # state = relationship("State", back_populates="districts")
    # blocks = relationship("Block", back_populates="district", cascade="all, delete-orphan")
    
    def __repr__(self):
        value = getattr(self, "District")  # Replace 'name' with actual column name
        return f"<District(name='{value}')>"

class Block(Base):
    __table__ = reflected_tables['blocks']

    __mapper_args__ = {
        'primary_key': [__table__.c.id]  # Assuming 'id' is the primary key
    }
    
    # Foreign Key to District
    # district_id = Column(Integer, ForeignKey("districts.id"))
    
    # Relationships
    # district = relationship("District", back_populates="blocks")
    # panchayats = relationship("Panchayat", back_populates="block", cascade="all, delete-orphan")
    
    def __repr__(self):
        value = getattr(self, "Block")  # Replace 'name' with actual column name
        return f"<Block(name='{value}')>"

class Panchayat(Base):
    __table__ = reflected_tables['panchayats']

    __mapper_args__ = {
        'primary_key': [__table__.c.id]  # Assuming 'id' is the primary key
    }
    
    # Foreign Key to Block
    # block_id = Column(Integer, ForeignKey("blocks.id"))
    
    # Relationships
    # block = relationship("Block", back_populates="panchayats")
    
    def __repr__(self):
        value = getattr(self, "Panchayat")  # Replace 'name' with actual column name
        return f"<Panchayat(name='{value}')>"
