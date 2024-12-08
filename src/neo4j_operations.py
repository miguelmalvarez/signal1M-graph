import os
import json
from typing import Dict, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

class Neo4jConnection:
    def __init__(self):
        load_dotenv()
        
        # Neo4j credentials from environment variables
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password)
        )

    def close(self):
        self.driver.close()
    
    def _create_entity_nodes(self, tx, entities: Dict, article_id: str):
        """Create entity nodes and their relationships to articles"""
        for entity_type, entities_list in entities.items():
            for entity in entities_list:
                tx.run("""
                    MERGE (e:Entity {name: $name, type: $type})
                    WITH e
                    MATCH (a:Article {id: $article_id})
                    MERGE (e)-[:MENTIONED_IN]->(a)
                """, name=entity, type=entity_type, article_id=article_id)

    def _create_cooccurrence_relationships(self, tx, entities: Dict):
        """Create relationships between entities that co-occur in the same article"""
        for type1, entities1 in entities.items():
            for type2, entities2 in entities.items():
                if type1 >= type2:  # Avoid duplicate relationships
                    continue
                for entity1 in entities1:
                    for entity2 in entities2:
                        tx.run("""
                            MATCH (e1:Entity {name: $entity1, type: $type1})
                            MATCH (e2:Entity {name: $entity2, type: $type2})
                            MERGE (e1)-[r:CO_OCCURS_WITH]->(e2)
                            ON CREATE SET r.weight = 1
                            ON MATCH SET r.weight = r.weight + 1
                        """, entity1=entity1, type1=type1,
                             entity2=entity2, type2=type2)

    def _create_entities_and_relations(self, tx, entities: Dict, article_id: str):
        """Helper method to create entities and relations in a single transaction"""
        # Create article node
        tx.run("""
            MERGE (a:Article {id: $article_id})
        """, article_id=article_id)
        
        self._create_entity_nodes(tx, entities, article_id)
        self._create_cooccurrence_relationships(tx, entities)


    def load_entities_and_relations(self, file_path: str):
        """Load entities and their relations from a jsonl file into Neo4j"""
        with self.driver.session() as session:
            with open(file_path, 'r') as f:
                for line in f:
                    data = json.loads(line)
                    article_id = data.get('id')
                    entities = data.get('entities', {})
                    relations = data.get('relations', {})

                    if article_id and entities:
                        session.execute_write(
                            self._create_entities_and_relations,
                            entities,
                            article_id,
                            relations
                        )

def load_entity_file(file_path: str):
    """
    Load a single entity file from data/output into Neo4j
    
    Args:
        file_path (str): Path to the entity file to load
    """
    neo4j = Neo4jConnection()
    
    try:
        if os.path.exists(file_path):
            print(f"Loading entities from {file_path}")
            neo4j.load_entities_and_relations(file_path)
            print(f"Finished loading {file_path}")
        else:
            print(f"File not found: {file_path}")
                
    finally:
        neo4j.close()

if __name__ == "__main__":
    load_entity_file('data/output/entities-10-vc.jsonl') 