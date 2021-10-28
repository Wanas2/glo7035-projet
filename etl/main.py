from pymongo import MongoClient, DESCENDING
from datetime import datetime
from os import listdir, path
from typing import Tuple
from hashlib import md5
import json

FILENAME = "source_file"
NOMCOMPLET = "nomComplet"
MATRICULE = "matricule"
STATUS = "file_status"
HASH = "hash"
MESSAGE = "message"

DATAPATH = path.abspath(path.join(path.dirname(__file__),'data2'))
mongo_client = MongoClient(host="mongo_service")
log_collection_pointer = mongo_client['log']
restaurants_collection_pointer = mongo_client['restaurants']


def get_source_file(data_source_path: str) -> Tuple[str, list]:
    for file in listdir(data_source_path):
        file_handle = open(path.join(data_source_path, file))
        yield file, file_handle


def get_log_document_template():
    return {
        "date": datetime.now()
    }


def get_file_last_hash(filename: str) -> str:
    file_hash_pointer = log_collection_pointer.find({FILENAME: filename}, {HASH: 1}).sort("_id", DESCENDING)
    if file_hash_pointer.count() == 0:
        file_hash = ""
    else:
        document = file_hash_pointer.next()
        if HASH in document:
            return document[HASH]
        else:
            file_hash = ""
    return file_hash


def file_to_update(filename : str, file_hash : str) -> bool:
    last_hash = get_file_last_hash(filename)
    return file_hash != last_hash


def insert_log(log_document: dict):
    log_collection_pointer.insert_one(log_document)

def insert_restaurant(source_file: str, restaurant_document : dict):
    restaurant_document[FILENAME] = source_file
    restaurants_collection_pointer.insert_one(restaurant_document)

def delete_restaurant(source_file: str):
    restaurants_collection_pointer.delete_many({FILENAME:source_file})


if __name__ == "__main__":
    for filename, file_handle in get_source_file(DATAPATH):

        log_document = get_log_document_template()
        log_document[FILENAME] = filename
        try:
            restaurant_document = json.load(file_handle)
            current_document_hash = md5(str(restaurant_document).encode()).hexdigest()
            if file_to_update(filename, current_document_hash):
                delete_restaurant(filename)
                for restaurant in restaurant_document:
                    insert_restaurant(filename, restaurant)
                log_document[STATUS] = "EXTRACTED"
                log_document[HASH] = current_document_hash
                print("Lecture complétée du fichier {}".format(filename))

            else:
                log_document[STATUS] = "IGNORED"
                log_document[HASH] = current_document_hash
                print("Aucun changement détecté dans le fichier {}".format(filename))
        except Exception as e:
            print("Erreur avec le document {}: {}".format(filename, e.args[0]))
            log_document[MESSAGE] = e.args[0]
            log_document[STATUS] = "ERROR"

        if log_document[STATUS] in ["EXTRACTED", "ERROR"]:
            insert_log(log_document)
