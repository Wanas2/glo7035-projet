import json
import neo4j
import os
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class Neo4j:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_segments(self,data_path,file_name):
        value=self.get_json(data_path,file_name)
        with self.driver.session() as session:
            result = session.write_transaction(self.create_return_graph, value)
            session.close()
            print("we represent the following segments in neo4j: segment{p}......".format(p=result[0:19]))

    @staticmethod
    def get_json(data_path,file_name):
        file_path = os.path.join(data_path, file_name)
        with open(file_path) as data_file:
            my_json = json.load(data_file)
            return my_json
    
    def create_return_graph(self, tx, value):
        query = ("unwind $value.features as features unwind features.geometry as geometry unwind features.properties as properties unwind properties.TOPONYMIE as TOPONYMIE unwind properties.SHAPE_Length as SHAPE_Length unwind properties.NOMGENERIQUE as NOMGENERIQUE merge (p:Point{longitude:geometry.coordinates[0][0],latitude:geometry.coordinates[0][1]}) merge(o:Point{longitude:geometry.coordinates[-1][0],latitude:geometry.coordinates[-1][1]}) merge (p)-[s:segment{TOPONYMIE:TOPONYMIE,SHAPE_Length:SHAPE_Length,NOMGENERIQUE:NOMGENERIQUE}]->(o) return s")
        result = tx.run(query, value=value)
        try:
            return [record["s"]["NOMGENERIQUE"]
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

            
if __name__ == "__main__":
    scheme = "bolt" 
    host_name = "localhost"
    port = 7687
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "neo4j"
    password = "password"
    data_path = "/data/"
    file_name = "segments.geojson"
    driver = Neo4j(url, user, password)
    driver.import_segments(data_path, file_name)
    driver.close()
