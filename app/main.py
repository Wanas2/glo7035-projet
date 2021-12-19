from pymongo import MongoClient, DESCENDING
from flask import Flask, jsonify, render_template_string, redirect, request
import os
import json
from typing import Tuple
from neo4j import GraphDatabase
import markdown
from random import randint
import geojson

flask_env = os.getenv('flask_env')
application = Flask('test_app')

mongo_client = MongoClient(host="mongo_service")
log_collection_pointer = mongo_client['7035Projet']['log']
restaurants_collection_pointer = mongo_client['7035Projet']['restaurants']

driver1 = GraphDatabase.driver("bolt://neo4j_service:7687", auth=("neo4j", "password"))
 
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
    return redirect("/readme")
                           
@application.route('/heartbeat')
def heartbeat():
    return jsonify({
        "villeChoisie" : "Sherbrooke"
    })

@application.route('/extracted_data')
def extracted_data():
    nb_restaurants = restaurants_collection_pointer.count_documents({})

    with driver1.session() as session:
        result=list(session.run('match ()-[a]->() return count(a)'))
        nb_segments=result[0]['count(a)'] 
        session.close()  

    return jsonify({
        "nbRestaurants" : nb_restaurants,
        "nbSegments" : nb_segments
    })
    

def get_restaurants_per_category() -> Tuple[str, int]:
    categoriesDistinct = restaurants_collection_pointer.distinct('CategoriesList')
    for category in categoriesDistinct:
        nbRestaurants = restaurants_collection_pointer.count_documents({'CategoriesList': category})
        yield category, nbRestaurants

@application.route('/transformed_data')
def transformed_data():

    categoriesDict = {}
    for category, n in get_restaurants_per_category():
        categoriesDict[category] = n

    with driver1.session() as session:
        result=list(session.run('match ()-[a:segment]-() where a.VITESSE<=50 return sum(a.SHAPE_Length)'))
        longueurCyclable = result[0]['sum(a.SHAPE_Length)']
        session.close()

    return jsonify({
        "restaurants" : categoriesDict,
        "longueurCyclable": longueurCyclable
    })
    
@application.route('/readme')
def readme():
    with open('README.md', 'r') as f:
        md = markdown.markdown(f.read())
    return render_template_string(md)

@application.route('/type')
def type():
    categories = restaurants_collection_pointer.distinct('CategoriesList')
    return jsonify(categories)


@application.route('/starting_point')
def starting_point():
    with driver1.session() as session:

        params = json.loads(request.get_data(as_text=True))

        try:
            length = params['length']
            types = params['type']
        except:
            return "Paramètres nécessaires: length (int), type [string, ...]", 400

        if not isinstance(length, int):
            return "length doit être un int", 400

        if not isinstance(types, list):
            return "type doit être un list", 400
        else:
            for type in types:
                if not isinstance(type, str):
                    return "type doit être un list de string", 400

        query = "MATCH (r:Restaurant)-[POSITION]->(p:Point) RETURN p"
        results = list(session.run(query))

        random_index = randint(0, len(results))

        latitude = results[random_index][0]['latitude']
        longitude = results[random_index][0]['longitude']

        starting_point = geojson.Point((longitude, latitude))

        session.close()

    return jsonify({ "startingPoint": starting_point })

application.run('0.0.0.0',port, debug=debug)