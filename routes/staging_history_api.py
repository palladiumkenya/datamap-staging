from fastapi import  Depends, HTTPException
# from bson.json_util import dumps

from sqlalchemy.orm import sessionmaker, Session
import uuid

from pydantic import BaseModel, Field
from typing import Dict, Any
from datetime import datetime
import json
from fastapi import APIRouter
from typing import List

import logging

from settings import settings
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from models.models import Manifests, DataDictionaries
from database import database
from serializers.data_dictionary_serializer import data_dictionary_list_entity


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




router = APIRouter()


# # Create an inspector object to inspect the database
engine = None
inspector = None

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    return database.get_database()
#
# HOSTS = [settings.CASSANDRA_HOST]
# CREDENTIAL = {'username': settings.CASSANDRA_USER, 'password': settings.CASSANDRA_PASSWORD}
# AUTH_PROVIDER = PlainTextAuthProvider(username=settings.CASSANDRA_USER, password=settings.CASSANDRA_PASSWORD)


class Manifest(BaseModel):
    usl_repository_name: str = Field(..., description="Type of the database (e.g., 'mysql', 'postgresql')")
    count: int = Field(..., description="Count of data extracted and sent from source")
    columns: List[str] = Field(..., description="Base repo column names")
    session_id: str = Field(..., description="Id of session for transmission")
    source_system_name: str = Field(..., description="Name of the syet sending the data")
    source_system_version: str = Field(..., description="Version of the syet sending the data")
    opendive_version: str = Field(..., description="Database password")
    facility: str = Field(..., description="Facility name")


@router.get('/usl/{baselookup}')
async def stage_usl_data(baselookup:str, db=Depends(get_db)):
    try:
        collection = db["manifests"]

        manifests = collection.find({"usl_repository_name": baselookup})

        return json.loads(dumps(manifests))
    except Exception as e:
        return {"status":500, "message":e}

@router.get('/base_schemas')
async def base_schemas():
    try:
        schemas = DataDictionaries.objects().all()
        schemas =data_dictionary_list_entity(schemas)
        return schemas
    except Exception as e:
        log.error('System ran into an error --> ', e)
        return e

def convert_to_iso(value):
    try:
        # Try parsing the date string in common formats and convert to ISO
        parsed_date = datetime.strptime(value, "%d/%m/%Y")  # Example format: DD/MM/YYYY
        return parsed_date.isoformat()  # Convert to ISO format
    except ValueError:
        try:
            # Try another common date format
            parsed_date = datetime.strptime(value, "%Y-%m-%d")  # Example format: YYYY-MM-DD
            return parsed_date.isoformat()
        except ValueError:
            # If parsing fails, return the original value (it's not a date)
            return value





def convert_datetime_to_iso(data):
    if isinstance(data, dict):
        return {k: convert_datetime_to_iso(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_datetime_to_iso(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data



def convert_none_to_null(data):
    if isinstance(data, dict):
        return {k: convert_none_to_null(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_none_to_null(item) for item in data]
    elif data is None:
        return None # This will be converted to null in JSON
    else:
        return data

def convert_datetime_to_iso(data):
    if isinstance(data, dict):
        return {k: convert_datetime_to_iso(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_datetime_to_iso(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data



