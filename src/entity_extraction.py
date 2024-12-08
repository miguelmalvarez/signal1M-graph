import json
import spacy
from time import sleep
from pathlib import Path
from typing import Dict, List, Generator, Tuple
from spacy_llm.util import assemble
from spacy.tokens import Doc
import os
from dotenv import load_dotenv

load_dotenv()  # This loads the .env file

def load_spacy_model():
    """Load spaCy model with LLM relationship extraction"""
    nlp = assemble('src/spacy.cfg')
    return nlp

def process_line(text: str, nlp) -> Dict:
    """Extract entities and relationships from a single text"""
    print(len(text))


    relevant_entity_types = ["GPE", "ORG", "PERSON"]
    try:
        doc = nlp(text)
        
        # Extract entities
        entities = {}
        for ent in doc.ents:
            if ent.label_ in relevant_entity_types:
                if ent.label_ not in entities:
                    entities[ent.label_] = []
                if ent.text not in entities[ent.label_]:
                    entities[ent.label_].append(ent.text)
        
        # Extract relationships
        relationships = []
        for rel in doc._.rel:
            if doc.ents[rel.dep].label_ in relevant_entity_types and doc.ents[rel.dest].label_ in relevant_entity_types:
                relationships.append((str(doc.ents[rel.dep]), str(rel.relation), str(doc.ents[rel.dest])))

        return {
            'entities': entities,
            'relationships': relationships
        }

    except Exception as e: 
        print("Problem: Sleeping for 5 seconds")
        print(f"Error: {type(e).__name__}: {str(e)}")
        sleep(5)
        

def process_jsonl(input_path: Path) -> Generator[Dict, None, None]:
    """Process each line of the JSONL file and extract entities and relationships"""
    nlp = load_spacy_model()
    
    #TODO: Add coreference resolution step?
    with open(input_path) as f:
        for line in f:
            article = json.loads(line)
            text = article['content']
            
            # Only processing News articles to ensure higher quality
            if article['media-type'] == "News":
                print(f"\nProcessing article {article['title']}")

                #TODO: Potentially using summarisation to limit the amount of text processed
                # Split texts when they are too long for the LLM
                max_char_size = 3000
                extractions = {'entities': {}, 'relationships': []}
                print(len(text))

                num_chunks = len(text)//max_char_size
                if len(text)%max_char_size > 0:
                   num_chunks += 1
                
                for i in range(0, num_chunks):
                    chunk = text[(i*max_char_size):(i+1)*max_char_size] # TODO: Chunking this more cleverly
                    chunk_extractions = process_line(chunk, nlp)
                    
                    # Merge entities dictionaries
                    for entity_type, entities in chunk_extractions['entities'].items():
                        if entity_type not in extractions['entities']:
                            extractions['entities'][entity_type] = []
                        extractions['entities'][entity_type].extend(entities)
                    
                    # Merge relationships lists
                    extractions['relationships'].extend(chunk_extractions['relationships'])
                
                yield {
                    'id': article['id'],
                    'title': article['title'],
                    'entities': extractions['entities'],
                    'relationships': extractions['relationships']
                }

def extract_entities(input_path: Path, output_path: Path):
    """Extract entities and relationships from all articles and save to output file"""
    print(f"Extracting entities and relationships from {input_path}")
    
    with open(output_path, 'w') as f:
        for result in process_jsonl(input_path):
            json.dump(result, f)
            f.write('\n')
    
    print(f"Entities and relationships saved to {output_path}")

if __name__ == "__main__":
    markets = ['legal', 'vc']
    sample_sizes = [100]

    for market in markets:
        for size in sample_sizes:
            input_path = Path(f"data/processed/signalmedia-{size}-{market}.jsonl")
            output_path = Path(f"data/output/entities-{size}-{market}.jsonl")
            extract_entities(input_path, output_path)
