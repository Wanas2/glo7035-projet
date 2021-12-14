import os
import json

from neo4j import GraphDatabase
from pymongo import MongoClient

class Neo4JClient:
    def __init__(self, uri, auth=('neo4j','password')):
        self.drive = None
        self.session = None

        try:
            self.driver = GraphDatabase.driver(uri, auth=auth)
        except Exception as e:
            print("Failed to create the driver: {}".format(e.args[0]))


    def close(self):
        if self.driver:
            self.driver.close()

    def open_session(self):
        return self.driver.session()
    
    def close_session(self):
        if self.session:
            self.session.close()

    def query(self, query, parameters=None):
        assert self.driver is not None, "Driver not initialized!"
        try:
            session = self.open_session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Failed to run query: {} - {}".format(query, e.args[0]))
        self.close_session()
        return response

    def write_transaction(self, func, value):
        assert self.driver is not None, "Driver not initialized!"
        try:
            session = self.open_session() 
            response = list(session.write_transaction(func, value))
        except Exception as e:
            print("Failed to write transaction {}".format(e.args[0]))
        self.close_session()
        return response


neo4j_client = Neo4JClient("bolt://neo4j_service:7687")


def get_json(file_directory, file_name):
    file_path = os.path.join(file_directory, file_name)
    try:
        with open(file_path) as file:
            return json.load(file)
    except Exception as e:
        print("Problem with file {} : {}".format(file_path, e.args[0]))


def create_return_graph(tx, value):
    query = ("unwind $value.features as features unwind features.geometry as geometry unwind features.properties as properties unwind properties.TOPONYMIE as TOPONYMIE unwind properties.VITESSE as VITESSE unwind properties.SHAPE_Length as SHAPE_Length unwind properties.NOMGENERIQUE as NOMGENERIQUE merge (p:Point{longitude:geometry.coordinates[0][0],latitude:geometry.coordinates[0][1]}) merge(o:Point{longitude:geometry.coordinates[-1][0],latitude:geometry.coordinates[-1][1]}) merge (p)-[s:segment{TOPONYMIE:TOPONYMIE,SHAPE_Length:SHAPE_Length,NOMGENERIQUE:NOMGENERIQUE,VITESSE:VITESSE}]->(o) return s")

    try:
        result = tx.run(query, value=value)
        return [record["s"]["NOMGENERIQUE"] for record in result]
    except Exception as e:
        print("Problem with query {} : {}".format(query, e.args[0]))


def add_restaurant(name, types, latitude, longitude):
    query = ("MATCH (p:Point) WITH point({ longitude:" + f"{longitude}"+",latitude:" + f"{latitude}" +" }) AS restaurant, point({ longitude: p.longitude, latitude: p.latitude}) AS neighbour RETURN distance(restaurant, neighbour) AS dist, neighbour.latitude, neighbour.longitude ORDER BY dist LIMIT 1")
    result = neo4j_client.query(query)[0]
    latitude = result['neighbour.latitude']
    longitude = result['neighbour.longitude']

    query = "CREATE (r:Restaurant {Nom: \"" + f"{name}" + "\", Categories: " + f"{types}" + "})"
    neo4j_client.query(query)

    query = ("MATCH (p:Point {latitude: " + f"{latitude}, longitude: {longitude}" + "}), (r:Restaurant {Nom: \"" + f"{name}" + "\"}) WHERE NOT (p)-[:est_proche_de]->(r) CREATE (p)-[:est_proche_de]->(r)")
    neo4j_client.query(query)


def clean():
    query = "MATCH (p:Point) RETURN p"
    results = neo4j_client.query(query)
        
    for result in results:
        for node in result:
            latitude = node['latitude']
            longitude = node['longitude']
            if not(isinstance(latitude, float) and isinstance(longitude, float)):
                query = f"MATCH (p:Point) WHERE p.latitude = {latitude} AND p.longitude = {longitude} DETACH DELETE p"
                results = neo4j_client.query(query)


def import_segments(file_directory, file_name):
    file = get_json(file_directory, file_name)

    try:
        result = neo4j_client.write_transaction(create_return_graph, file)
        print("we represent the following segments in neo4j: segment{p}......".format(p=result[0:19]))        
    except Exception as e:
        print("Problem with driver session : {}".format(e.args[0]))
    
    clean()
            
if __name__ == "__main__":
    import_segments("/data/segments", "Segments.geojson")

    mongo_client = MongoClient(host="mongo_service")
    restaurants_collection_pointer = mongo_client['7035Projet']['restaurants']
    restaurants = restaurants_collection_pointer.find({})

    for restaurant in restaurants:
        latitude = restaurant["Latitude"]
        longitude = restaurant["Longitude"]
        nom = restaurant["Nom"]
        types = restaurant["CategoriesList"]
        if latitude and longitude:
            add_restaurant(nom, types, latitude, longitude)

    neo4j_client.close()