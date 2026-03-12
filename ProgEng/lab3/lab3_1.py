import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USERNAME')
password = os.getenv('NEO4J_PASSWORD')
database = os.getenv('NEO4J_DBNAME')

driver = GraphDatabase.driver(uri, auth=(user, password))

def execute_query(query):
    with driver.session(database=database) as session:
        result = session.run(query)
        return [record for record in result]

query = """
CREATE (p:Person {name: 'Bob'})
RETURN p
"""
results = execute_query(query)
for record in results:
    print(record["p"])

driver.close()