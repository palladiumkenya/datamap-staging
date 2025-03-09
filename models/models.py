import uuid
from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    DateTime,
    create_engine,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime


Base = declarative_base()




class UniversalDictionaryConfig(Base):
    __tablename__ = 'UniversalDictionaryConfig'

    id =  Column(UUID(as_uuid=True),primary_key=True, default=uuid.uuid1)
    universal_dictionary_url = Column(String,nullable=False)
    universal_dictionary_jwt = Column(String, nullable=False)
    universal_dictionary_update_frequency = Column(String, nullable=True)
    created_at = Column(DateTime,nullable=False, default=datetime.utcnow())
    updated_at = Column(DateTime,nullable=False, default=datetime.utcnow())
    deleted_at = Column(DateTime,nullable=True)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()


class DataDictionaries(Base):
    __tablename__ = "DataDictionaries"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    name = Column(String,nullable=False)
    is_published = Column(Boolean, default=False)
    version_number = Column(Integer,nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    deleted_at = Column(DateTime, nullable=True)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

    class Settings:
        collection = "data_dictionaries"


class DataDictionaryTerms(Base):
    __tablename__ = "DataDictionaryTerms"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    dictionary = Column(String, nullable=False)
    dictionary_id = Column(UUID(as_uuid=True), nullable=False)
    term = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    is_required = Column(Boolean, default=False)
    term_description = Column(String, nullable=True)
    expected_values = Column(String, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    deleted_at = Column(DateTime, nullable=True)

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

    class Settings:
        collection = "data_dictionary_terms"






class Facilities(Base):
    __tablename__ = "Facilities"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    facility_name = Column(UUID(as_uuid=True), default=uuid.uuid1)
    site_code = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow())

    def save(self):
        self.updated_at = datetime.utcnow()
        super().save()

    class Settings:
        collection = "facilities"


class Manifests(Base):
    __tablename__ = "Manifests"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1)
    facility_name = Column(String, nullable=False)
    facility_id = Column(String, nullable=False)
    manifest_id = Column(UUID(as_uuid=True), default=uuid.uuid1)
    usl_repository_name = Column(String, nullable=False)
    expected_count = Column(Integer)
    received_count = Column(Integer, nullable=True)
    source_system_name = Column(String, nullable=False)
    source_system_version = Column(String, nullable=False)
    opendive_version = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())
    session_id = Column(UUID(as_uuid=True), default=uuid.uuid1)
    start = Column(DateTime, nullable=False, default=datetime.utcnow())
    end = Column(DateTime, nullable=True)
    status = Column(String)

    def save(self):
        self.created_at = datetime.utcnow()
        super().save()

    class Settings:
        collection = "manifests"



# Dynamically create models
from database.create_dictionary_models import *
dynamic_models = create_models_from_metadata()