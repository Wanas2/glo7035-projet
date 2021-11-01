from pymongo import MongoClient, DESCENDING
from flask import Flask, jsonify
import os
import json
from typing import Tuple

flask_env = os.getenv('flask_env')
application = Flask('test_app')

mongo_client = MongoClient(host="mongo_service")
log_collection_pointer = mongo_client['7035Projet']['log']
restaurants_collection_pointer = mongo_client['7035Projet']['restaurants']

if flask_env == "prod":
    debug = False
    port = 80
elif flask_env == "dev":
    debug = True
    port = 8080
else :
    raise ValueError("Aucun comportement attendu pour l'environnement {}".format(flask_env))

@application.route('/')
def index():
    return("<p>Hello world!</p>")
                           
@application.route('/heartbeat')
def heartbeat():
    return jsonify({
        "villeChoisie" : "Sherbrooke"
    })

@application.route('/extracted_data')
def extracted_data():
    nb_restaurants = restaurants_collection_pointer.count()
    return jsonify({
        "nbRestaurants" : nb_restaurants
    })

def get_restaurants_per_category() -> Tuple[str, int]:
    categoriesDistinct = restaurants_collection_pointer.distinct('CategoriesList')
    for category in categoriesDistinct:
        nbRestaurants = restaurants_collection_pointer.count({'CategoriesList': category})
        yield category, nbRestaurants

@application.route('/transformed_data')
def transformed_data():

    categoriesDict = {}
    for category, n in get_restaurants_per_category():
        categoriesDict[category] = n

    return jsonify({
        "restaurants" : categoriesDict
    })

application.run('0.0.0.0',port, debug=debug)