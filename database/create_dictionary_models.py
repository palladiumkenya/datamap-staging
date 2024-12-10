
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
from database.create_dictionary_models import *



DATA_TYPE_MAP = {
    "int": Integer,
    "nvarchar": String,
    "float": Float,
    "datetime2": DateTime,
    "boolean": Boolean,
}

# db: Session = Depends(get_db)
def create_models_from_metadata():
    # Query metadata tables
    session = Session(engine)
    tables = session.query(DataDictionaries).all()
    columns = session.query(DataDictionaryTerms).all()
    session.close()

    models = {}

    for table in tables:
        table_name = table.name

        # Get columns for this table
        table_columns = [
            col for col in columns if col.dictionary == table_name
        ]

        # Define table fields
        fields = {
            "__tablename__": table_name,
            "id": Column(UNIQUEIDENTIFIER, primary_key=True, default=uuid.uuid1),  # Add default id column
        }

        for col in table_columns:
            col_type = DATA_TYPE_MAP.get(col.data_type.lower(), String)  # Default to String
            fields[col.term] = Column(col_type)

        # Dynamically create a model class
        model = type(table_name, (Base,), fields)
        models[table_name] = model

    return models