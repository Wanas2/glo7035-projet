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
    
    def raw_query(self, query, parameters=None):
        assert self.driver is not None, "Driver not initialized!"
        try:
            session = self.open_session() 
            response = session.run(query, parameters)
        except Exception as e:
            print("Failed to run query: {} - {}".format(query, e.args[0]))
        self.close_session()
        return response

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


def add_restaurant(name, types, latitude, longitude, client):
    lat, long = latitude, longitude
    
    query = ("MATCH (p:Point) WITH point({ longitude:" + f"{longitude}"+",latitude:" + f"{latitude}" +" }) AS restaurant, point({ longitude: p.longitude, latitude: p.latitude}) AS neighbour RETURN distance(restaurant, neighbour) AS dist, neighbour.latitude, neighbour.longitude ORDER BY dist LIMIT 1")
    result = client.query(query)[0]
    latitude = result['neighbour.latitude']
    longitude = result['neighbour.longitude']

    query = "MERGE (r:Restaurant {Nom: \"" + f"{name}" + "\", Categories: " + f"{types}" + ", Position:["+ str(lat) + "," + str(long) +"]})"
    client.query(query)

    query = ("MATCH (p:Point {latitude: " + f"{latitude}, longitude: {longitude}" + "}), (r:Restaurant {Nom: \"" + f"{name}" + "\"}) WHERE NOT (p)-[:est_proche_de]->(r) MERGE (p)<-[:est_proche_de]-(r)")
    client.query(query)


def total_shape_length(path):
    total = 0
    for r in path.relationships:
        if "SHAPE_Length" in r:
            total = total + r["SHAPE_Length"]
    return total

def line_string(path):
    line = []
    for n in path.nodes:
        label = list(n.labels)[0]
        if label == "Point":
            line.append(n.get("latitude"))
            line.append(n.get("longitude"))
    return line  

# def calculate_restaurants_path(client):
#     res = client.raw_query("MATCH (r:Restaurant) RETURN r")
#     graph = res.graph()
#     for ix, node in enumerate(graph.nodes):
#         print(f"\n\n======== Node: {ix}")
#         res = client.raw_query("MATCH p=(:Restaurant{Nom:\""+node.get("Nom")+"\"})-[:est_proche_de]-(:Point)-[:segment*1..10]->(:Point)-[:est_proche_de]-(:Restaurant) RETURN p")
#         for record in res:
#             path = record["p"]
#             length = total_shape_length(path)
#             line = line_string(path)

#             if len(line) > 0:
#                 query = "MATCH (a:Restaurant),(b:Restaurant) WHERE NOT a.Nom = b.Nom AND a.Nom = \""+ path.start_node.get("Nom") +"\" AND b.Nom = \""+ path.end_node.get("Nom") +"\" AND NOT (a)-[:chemin]-(b) CREATE (a)-[r:chemin {length:\""+ str(length) +"\", line_string:"+ str(line) +"}]->(b) RETURN type(r)"
#                 client.raw_query(query)

#                 print(record["p"])


def clean(client):
    query = "MATCH (p:Point) RETURN p"
    results = client.query(query)
        
    for result in results:
        for node in result:
            latitude = node['latitude']
            longitude = node['longitude']
            if not(isinstance(latitude, float) and isinstance(longitude, float)):
                query = f"MATCH (p:Point) WHERE p.latitude = {latitude} AND p.longitude = {longitude} DETACH DELETE p"
                results = client.query(query)


def import_segments(file_directory, file_name, client):
    file = get_json(file_directory, file_name)

    try:
        result = client.write_transaction(create_return_graph, file)
        print("we represent the following segments in neo4j: segment{p}......".format(p=result[0:19]))        
    except Exception as e:
        print("Problem with driver session : {}".format(e.args[0]))
    
    clean(client=client)
            
def start_neo4j_etl():
    neo4j_client = Neo4JClient("bolt://neo4j_service:7687")
    import_segments("/data/segments", "Segments.geojson", client=neo4j_client)

    mongo_client = MongoClient(host="mongo_service")
    restaurants_collection_pointer = mongo_client['7035Projet']['restaurants']
    restaurants = restaurants_collection_pointer.find({})

    for restaurant in restaurants:
        latitude = restaurant["Latitude"]
        longitude = restaurant["Longitude"]
        nom = restaurant["Nom"]
        types = restaurant["CategoriesList"]
        if latitude and longitude:
            add_restaurant(nom, types, latitude, longitude, client=neo4j_client)

    # calculate_restaurants_path(neo4j_client)

    neo4j_client.close()