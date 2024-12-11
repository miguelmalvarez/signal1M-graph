# Relationship Extraction with Spacy-llm and Neo4j

## Project Description
Project to showcase space and spacy-llm entity and relationship extraction, as well as  neo4j for graphDB. 

## Project Structure
- `data/`: Contains all the data files
  - `raw/`: Original, immutable data
  - `processed/`: Sampled data
  - `output/`: Data with entities and relationships information
- `src/`: Source code for use in this project
- `scripts/`: Scripts for downloading spacy core models

## Setup
1. Clone this repository
2. Install requirements:   ```
   pip install -r requirements.txt   ```
3. Download spacy core models:   ```
   python scripts/setup.py   ```
4. Download and sample the dataset:   ```
   python src/dataset.py   ```
5. Run entity and relationship extraction:   ```
   python src/entity_extraction.py   ```
6. Load data into neo4j:   ```
   python src/neo4j_operations.py   ```