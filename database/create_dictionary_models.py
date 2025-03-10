from sqlalchemy import inspect, text
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
        inspector = inspect(engine)
        if inspector.has_table("DataDictionaries"):
            tables = session.query(DataDictionaries).all()
            columns = session.query(DataDictionaryTerms).all()
            # session.close()

            global models  # Allow modification of the global models dictionary
            models.clear()

            for table in tables:
                table_name = table.name.lower()

                # Get columns for this table
                table_columns = [
                    col for col in columns if col.dictionary == table_name
                ]

                # Define table fields
                fields = {
                    "__tablename__": table_name,
                    "__table_args__": {"extend_existing": True},
                    "id": Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid1),  # Add default id column
                    table_name+"_id": Column(String,nullable=True),  # Add table_name id column

                }

                for col in table_columns:
                    col_name = col.term.lower()
                    col_type = DATA_TYPE_MAP.get(col.data_type, String)  # Default to String
                    fields[col_name] = Column(col_type)
                    col_type_sql = str(col_type().compile(dialect=engine.dialect))

                    # if table already created then alter and add columns
                    if inspector.has_table(table_name):
                        existing_columns = {
                            col["name"].lower(): col["type"] for col in inspector.get_columns(table_name)
                        }

                        if col_name not in existing_columns:
                            # Add new column
                            alter_sql = f'ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type_sql}'
                            # engine.execute(text(alter_sql))
                            session.execute(text(alter_sql))
                        elif str(existing_columns[col_name]) != str(col_type):
                            # Modify column type if it has changed
                            alter_sql = f'ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {col_type_sql}'
                            # engine.execute(text(alter_sql))
                            session.execute(text(alter_sql))

                # session.commit()  # Ensure the change is saved
                session.close()

                # Dynamically create a model class
                model = type(table_name, (Base,), fields)
                models[table_name] = model
        return models
    except Exception as e:
        print("error creating dynamic tables -->", e)
        return models
