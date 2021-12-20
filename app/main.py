from pymongo import MongoClient, DESCENDING
from flask import Flask, jsonify, render_template, render_template_string, redirect, request
import os
import json
from typing import Tuple
from neo4j import GraphDatabase
import markdown
from random import randint
import geojson
import requests

flask_env = os.getenv('flask_env')
application = Flask('test_app',template_folder="templates")

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

def total_shape_length(path):
    total = 0
    for r in path.relationships:
        if "length" in r:
            total = total + float(r["length"])
    return total

def line_string(path):
    for r in path.relationships:
        if "line_string" in r:
            return r["line_string"]

@application.route("/parcours")
def parcours():
    with driver1.session() as session:
        params = json.loads(request.get_data(as_text=True))

        try:
            starting_point = geojson.loads(params["startingPoint"])
            nb_of_stops = params["numberOfStops"]
            length = params['length']
            types = params['type']

        except:
            return "Paramètres nécessaires: startingPoint(point), numberOfStops(int), length (int), type [string, ...]", 400

        if not isinstance(starting_point, geojson.Point):
            return "startingPoint doit être un point", 400

        if not isinstance(nb_of_stops, int):
            return "numberOfStops doit être un int", 400

        if not isinstance(length, int):
            return "length doit être un int", 400

        if not isinstance(types, list):
            return "type doit être un list", 400
        else:
            for type in types:
                if not isinstance(type, str):
                    return "type doit être un list de string", 400

        all_restaurants = []
        segments_parcours = []
        total_length = 0

        condition = ""
        if len(types) == 0:
            condition = "WITH a, a.Categories AS categories UNWIND categories AS category WITH a, category WHERE category IN "+ str(types)

        query = "MATCH (a:Restaurant)-[:est_proche_de]-(b:Point{latitude:"+ str(starting_point["coordinates"][0]) + ", longitude:"+ str(starting_point["coordinates"][1]) +"})"+ condition + " RETURN a.Nom"
        current = list(session.run(query))[0][0]
        application.logger.info(current)

        while total_length < length and len(all_restaurants) < nb_of_stops:
            condition = ""
            if len(types) == 0:
                condition = "WITH p, b.Categories AS categories UNWIND categories as category WITH p, category WHERE category IN "+ str(types)
            res = session.run("MATCH p=(a:Restaurant{Nom:\""+ current +"\"})-[r:chemin*]-(b:Restaurant) "+ condition +" RETURN p")

            path = None
            for record in res:
                path = record["p"]
                if len(path) > 0:
                    break
                
            if path:
                node = path.start_node
                if node not in all_restaurants:
                    p = geojson.Point(node.get("position"))
                    all_restaurants.append(geojson.Feature(geometry=p, properties={"name": node.get("Nom"), "type": str(node.get("Categories"))}))
                total_length = total_length + total_shape_length(path)
                raw_line = line_string(path)
                formated_line = []
                for ix in range(0, len(raw_line), 2):
                    formated_line.append([raw_line[ix], raw_line[ix+1]])
                l = geojson.LineString(formated_line)
                segments_parcours.append(geojson.Feature(geometry=l, properties={"length": total_shape_length(path)})) 
                current = path.end_node.get("Nom")

            else:
                break
        
    parcours = []
    for ix in range(len(all_restaurants)):
        parcours.append(all_restaurants[ix])
        parcours.append(segments_parcours[ix])
            
    return geojson.FeatureCollection(parcours)

@application.route('/map')
def map_func():
	return render_template('map.html')

@application.route('/getparcours/<name>')
def getParcours(name):
   basedir = os.path.abspath(os.path.dirname(__file__))
   result_file = os.path.join(basedir, 'templates/'+name)
   if os.path.exists(result_file):
         return render_template(name)
   else:
        return "vous n'avez pas choisi votre parcours"

@application.route('/weather')
def weather_func():
    url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=imperial&appid=45ed38051862c962bc68103892378b70'
    city="Sherbrooke"
    r = requests.get(url.format(city)).json()
    weather = {
            'city' : city,
            'temperature' : r['main']['temp'],
            'description' : r['weather'][0]['description'],
            'icon' : r['weather'][0]['icon'],
        }
    return render_template('weather.html', weather=weather)

@application.route('/forecast')
def forecast():
    url='https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&units=imperial&exclude=current,minutely,hourly,alerts&appid=45ed38051862c962bc68103892378b70'
    lat=45.4042
    lon=-71.8929
    r = requests.get(url.format(lat,lon)).json()
    weather_data = []
    count=0
    for day in r['daily']:
        count+=1
        weather = {
            'day': 'day {}'.format(count),
            'temperature' : day['temp']['day'],
            'description' : day['weather'][0]['description'],
            'icon' : day['weather'][0]['icon'],
        }

        weather_data.append(weather)
    return render_template('forecast.html',weather_data=weather_data)

application.run('0.0.0.0',port, debug=debug)