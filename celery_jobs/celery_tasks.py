from typing import List
from typing import Dict, Any
from datetime import datetime

from celery import Celery
from database import database
from fastapi import  Depends
from models.models import Manifests
from database.create_dictionary_models import *



CELERY_BROKER_URL = "amqp://guest:guest@localhost:5672//"
CELERY_RESULT_BACKEND = "rpc://"

celery = Celery("opendive_tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)


def get_db():
    return database.get_database()

models = create_models_from_metadata()


@celery.task(name="celery_jobs.celery_tasks.process_data")  # Explicitly define task name
def process_data(data: str):
    """Sample task to process data"""
    print(f"Processing data: {data}")
    return f"Processed: {data}"


@celery.task(name="celery_jobs.celery_tasks.process_usl_data")  # Explicitly define task name
def process_usl_data(baselookup:str,  usl_data: Dict[str, Any]):
    """process data"""
    try:
        db=get_db()

        USLDictionaryModel = models.get(baselookup)

        # collection = db[baselookup]
        #
        # collection.insert_many(usl_data["data"])

        count = collection.count_documents({"FacilityID":usl_data["facility_id"]})
        Manifests.objects(manifest_id=usl_data["manifest_id"]).update(received_count=count)

        if int(usl_data['batch_no']) == int(usl_data['total_batches']):
            Manifests.objects(manifest_id=usl_data["manifest_id"]).update(set__end=datetime.utcnow())
        return f"++++++++ Processed: {baselookup} USL data batch No. {usl_data['batch_no']} +++++++++"
    except Exception as e:
        return {"status":500, "message":e}