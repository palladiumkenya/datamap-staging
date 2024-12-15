import requests
import uuid
from collections import defaultdict
from uuid import UUID
import logging
from database.database import SessionLocal
from sqlalchemy.orm import Session

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from database import database
from models.models import DataDictionaries, DataDictionaryTerms,UniversalDictionaryConfig
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity, \
    data_dictionary_list_entity



log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()



def sync_dictionaries(usl_dicts: list) -> dict:
    dict_id_map = {}
    active_dicts = set()
    db: Session = SessionLocal()  # Create session from SessionLocal()

    for usl_dict in usl_dicts:
        active_dicts.add(usl_dict['dictionary']['name'])
        dictionary = usl_dict['dictionary']

        # existing_dict = DataDictionaries.objects().filter(name=dictionary['name']).allow_filtering().first()
        existing_dict = db.query(DataDictionaries).filter(getattr(DataDictionaries, "FacilityID", None) == dictionary['name']).first()
        if not existing_dict:
            new_dict = DataDictionaries(
                name=dictionary['name'].lower(),
                is_published=dictionary['is_published'],
                version_number=dictionary['version_number'],
            )
            db.add(new_dict)
            db.commit()  # Commit the changes

            for term in usl_dict['dictionary_terms']:
                term['dictionary_id'] = new_dict.id

        else:
            existing_dict.is_published = dictionary['is_published']
            existing_dict.version_number = dictionary['version_number']
            db.commit()  # Commit the changes

            dict_id_map[dictionary['name']] = existing_dict.id
            for term in usl_dict['dictionary_terms']:
                term['dictionary_id'] = existing_dict.id
        sync_terms(usl_dict['dictionary_terms'])
    # Deactivate dictionaries that are no longer present in usl_dicts
    # existing_dicts = DataDictionaries.objects().filter(datasource_id=datasource_id).allow_filtering()
    # for existing_dict in existing_dicts:
    #     if existing_dict.name not in active_dicts:
    #         DataDictionaries.objects(id=existing_dict.id).first().delete()

    return dict_id_map


def sync_terms(terms):
    db: Session = SessionLocal()  # Create session from SessionLocal()

    active_terms = set()
    dictionaries = []

    for usl_term in terms:
        dictionary_id = str(usl_term['dictionary_id'])
        dictionaries.append(dictionary_id)

        if dictionary_id:
            active_terms.add((usl_term['dictionary'], usl_term['term']))
            # existing_term = DataDictionaryTerms.objects().filter(dictionary=usl_term['dictionary'],
            #                                                      term=usl_term['term']).allow_filtering().first()
            existing_term = db.query(DataDictionaryTerms).filter(getattr(DataDictionaryTerms, "dictionary", None) == usl_term['dictionary'],
                getattr(DataDictionaryTerms, "term", None) == usl_term['term']).first()
            if not existing_term:
                new_term = DataDictionaryTerms(
                    dictionary=usl_term['dictionary'].lower(),
                    dictionary_id=dictionary_id,
                    term=usl_term['term'].lower(),
                    data_type=usl_term['data_type'],
                    is_required=usl_term['is_required'],
                    term_description=usl_term['term_description'],
                    expected_values=usl_term['expected_values'],
                    is_active=usl_term['is_active']
                )
                db.add(new_term)
                db.commit()
            else:
                existing_term.data_type = usl_term['data_type']
                existing_term.is_required = usl_term['is_required']
                existing_term.term_description = usl_term['term_description']
                existing_term.expected_values = usl_term['expected_values']
                existing_term.is_active = usl_term['is_active']
                db.commit()

    # Deactivate terms that are no longer present in usl_terms
    # existing_terms = DataDictionaryTerms.objects().filter(dictionary_id__in=dictionaries).allow_filtering()
    # for existing_term in existing_terms:
    #     if (existing_term.dictionary, existing_term.term) not in active_terms:
    #         DataDictionaryTerms.objects(id=existing_term.id).first().delete()
    return {"message": "Data dictionary terms synced successfully"}



# Function to create tables based on data dictionary terms
def create_tables():
    """
    Creates  tables based on data dictionary terms.
    :return: None
    """
    from database.create_dictionary_models import create_models_from_metadata

    create_models_from_metadata()



def pull_dict_from_universal(universal_dict_config):
    """
    Pull data dictionary from universal dictionary via api
    :param universal_dict_config: api config for universal dictionary
    :return: data dictionary
    """
    headers = {"Authorization": f"Bearer {universal_dict_config.universal_dictionary_jwt}"}
    response = requests.get(universal_dict_config.universal_dictionary_url, headers=headers)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.json())
    return response.json()


@router.get("/sync_all")
def sync_all(background_tasks: BackgroundTasks):
    db: Session = SessionLocal()  # Create session from SessionLocal()

    universal_dict_config = db.query(UniversalDictionaryConfig).first()
    if universal_dict_config is not None:
        response = pull_dict_from_universal(universal_dict_config)
        dict_map = sync_dictionaries(response.get("data"))
        background_tasks.add_task(create_tables)
        return {"message": "All data synced successfully", "data": dict_map}
    else:
        return {"message": "Please add a valid Universal Dictionary Configuration first"}



