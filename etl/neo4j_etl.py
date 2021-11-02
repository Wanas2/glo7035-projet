from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import os
import json

class Neo4jClient:

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def import_segments(self, data_path, file_name):
        value = self.get_json(data_path,file_name)
        try:
            with self.driver.session() as session:
                result = session.write_transaction(self.create_return_graph, value)
                session.close()
                print("we represent the following segments in neo4j: segment{p}......".format(p=result[0:19]))
        except Exception as e:
            print("Problem with driver session : {}".format(e.args[0]))

    @staticmethod
    def get_json(data_path, file_name):
        file_path = os.path.join(data_path, file_name)
        try:
            with open(file_path) as data_file:
                my_json = json.load(data_file)
                return my_json
        except Exception as e:
            print("Problem with file {} : {}".format(file_path, e.args[0]))

    def create_return_graph(self, tx, value):
        query = ("unwind $value.features as features unwind features.geometry as geometry unwind features.properties as properties unwind properties.TOPONYMIE as TOPONYMIE unwind properties.VITESSE as VITESSE unwind properties.SHAPE_Length as SHAPE_Length unwind properties.NOMGENERIQUE as NOMGENERIQUE merge (p:Point{longitude:geometry.coordinates[0][0],latitude:geometry.coordinates[0][1]}) merge(o:Point{longitude:geometry.coordinates[-1][0],latitude:geometry.coordinates[-1][1]}) merge (p)-[s:segment{TOPONYMIE:TOPONYMIE,SHAPE_Length:SHAPE_Length,NOMGENERIQUE:NOMGENERIQUE,VITESSE:VITESSE}]->(o) return s")
        try:
            result = tx.run(query, value=value)
            return [record["s"]["NOMGENERIQUE"]
                    for record in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

            
if __name__ == "__main__":
    neo4j_url = "bolt://neo4j_service:7687"
    neo4j_auth = ('neo4j','password')
    print("====== DEBUG", neo4j_url, neo4j_auth)

    data_path = "/data/segments"
    file_name = "Segmen.geojson"

    neo4j_client = Neo4jClient(neo4j_url, auth=neo4j_auth)
    neo4j_client.import_segments(data_path, file_name)
    neo4j_client.close()