from sqlalchemy import inspect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from routes import  staging_api, data_dictionary_api, staging_history_api
from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,Float,
    DateTime,
    create_engine,
)
from database import *
from models.models import *
from database.database import get_database, SessionLocal, engine
from sqlalchemy.orm import Session
from database.create_dictionary_models import DataDictionaries, DataDictionaryTerms
from sqlalchemy.dialects.postgresql import UUID



DATA_TYPE_MAP = {
    "int": Integer,
    "nvarchar": String,
    "float": Float,
    "datetime2": DateTime,
    "boolean": Boolean,
}


# Dictionary to hold created models
models = {}
def create_models_from_metadata():
    try:
        # Query metadata tables
        session = Session(engine)
        if inspect(engine).has_table("DataDictionaries"):
            tables = session.query(DataDictionaries).all()
            columns = session.query(DataDictionaryTerms).all()
            session.close()

            global models  # Allow modification of the global models dictionary

            for table in tables:
                table_name = table.name.lower()

                # Get columns for this table
                table_columns = [
                    col for col in columns if col.dictionary == table_name
                ]

                # Define table fields
                fields = {
                    "__tablename__": table_name,
                    "id": Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1),  # Add default id column
                    table_name+"_id": Column(String,nullable=True),  # Add table_name id column

                }

                for col in table_columns:
                    col_type = DATA_TYPE_MAP.get(col.data_type, String)  # Default to String
                    fields[col.term.lower()] = Column(col_type)

                # Dynamically create a model class
                model = type(table_name, (Base,), fields)
                models[table_name] = model
        return models
    except Exception as e:
        print("error creating dynamic tables -->", e)
        return models
