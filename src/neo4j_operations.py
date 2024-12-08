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
    
    def _create_entity_nodes(self, tx, entities: Dict):
        """Create entity nodes only
        
        Args:
            tx: Neo4j transaction
            entities (Dict): Dictionary of entity types to lists of entity names
        """
        for entity_type, entities_list in entities.items():
            for entity in entities_list:
                tx.run("""
                    MERGE (e:Entity {name: $name, type: $type})
                """, name=entity, type=entity_type)

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
    def _create_mentioned_in_relations(self, tx, entities: Dict, article_id: str):
        """Create MENTIONED_IN relationships between entities and articles
        
        Args:
            tx: Neo4j transaction
            entities (Dict): Dictionary of entity types to lists of entity names
            article_id (str): ID of the article to link entities to
        """
        for entity_type, entities_list in entities.items():
            for entity in entities_list:
                tx.run("""
                    MATCH (e:Entity {name: $name, type: $type})
                    MATCH (a:Article {id: $article_id})
                    MERGE (e)-[:MENTIONED_IN]->(a)
                """, name=entity, type=entity_type, article_id=article_id)

    def _create_extracted_relationships(self, tx, triples: List[tuple]):
        """Create relationships between entities based on (source, relation, target) triples
        
        Args:
            tx: Neo4j transaction
            triples: List of tuples, each containing (source, relation, target)
                    where source and target are entity names and relation is a string
        
        Example:
            triples = [
                ("Apple", "COMPETES_WITH", "Microsoft"),
                ("Google", "ACQUIRED", "YouTube"),
                ("Tesla", "LED_BY", "Elon Musk")
            ]
        """
        for source, relation, target in triples:
            query = f"""
                MATCH (e1:Entity {{name: $source}})
                MATCH (e2:Entity {{name: $target}})
                MERGE (e1)-[r:{relation.upper()}]->(e2)
                ON CREATE SET r.weight = 1
                ON MATCH SET r.weight = r.weight + 1
            """

            tx.run(query,
                source=source,
                target=target
            )

    def _create_entities_and_relations(self, tx, entities: Dict, relations: List[tuple], article_id: str):
        """Helper method to create entities and relations in a single transaction"""
        # Create article node
        tx.run("""
            MERGE (a:Article {id: $article_id})
        """, article_id=article_id)
        
        # Create entities first, then create relationships
        self._create_entity_nodes(tx, entities)
        # self._create_mentioned_in_relations(tx, entities, article_id)
        # self._create_cooccurrence_relationships(tx, entities)
        self._create_extracted_relationships(tx, relations)

    def load_entities_and_relations(self, file_path: str):
        """Load entities and their relations from a jsonl file into Neo4j"""
        with self.driver.session() as session:
            with open(file_path, 'r') as f:
                for line in f:
                    data = json.loads(line)
                    article_id = data.get('id')
                    entities = data.get('entities', {})
                    relations = data.get('relationships', {})

                    if article_id and entities:
                        session.execute_write(
                            self._create_entities_and_relations,
                            entities,
                            relations,
                            article_id
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
    load_entity_file('data/output/entities-1000-vc.jsonl') 