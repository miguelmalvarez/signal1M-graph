import spacy
import sys

def download_spacy_models():
    """Download required spaCy models"""
    for model in ["en_core_web_sm", "en_core_web_md", "en_core_web_lg"]:
        try:
            spacy.load(model)
            print(f"SpaCy model {model} already downloaded")
        except OSError:
            print(f"Downloading spaCy model {model}...")
            spacy.cli.download(model)
            print(f"Download complete for {model}")

if __name__ == "__main__":
    download_spacy_models()
    sys.exit(0) 