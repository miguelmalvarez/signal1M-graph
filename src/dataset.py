from pathlib import Path
from typing import List
import gzip
import requests
import json
import random
import re  # Add this to imports at the top

def download_file(url: str, target_path: Path) -> None:
    """Download file from url to target path"""
    print(f"Downloading dataset from {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    target_path.write_bytes(response.content)
    
    print(f"\nDownloaded to {target_path}")

def uncompress_dataset(input_path: Path, output_path: Path) -> None:
    """Uncompress gzipped dataset file"""
    print(f"Uncompressing {input_path}")
    
    with gzip.open(input_path, 'rb') as f_in:
        output_path.write_bytes(f_in.read())
    
    print(f"Uncompressed to {output_path}")

def create_sample(input_path: Path, output_path: Path, n_lines: int, keywords: List[str]) -> None:
    """Create a random sample of n_lines from input file, filtered by keywords if provided"""
    print(f"Creating random sample of {n_lines} lines with keywords {keywords}")
    
    # First pass: count total lines and collect indices of lines matching keywords
    valid_indices = []
    with open(input_path) as f:
        for i, line in enumerate(f):
            article = json.loads(line)
            # If keywords is empty or if any whole word keyword is found in content
            if not keywords or any(re.search(r'\b' + re.escape(keyword.lower()) + r'\b', 
                                           article['content'].lower()) 
                                 for keyword in keywords):
                valid_indices.append(i)
    
    # Randomly sample from valid indices
    if len(valid_indices) < n_lines:
        print(f"Warning: Only {len(valid_indices)} valid articles found, using all of them")
        selected_indices = valid_indices
    else:
        selected_indices = random.sample(valid_indices, n_lines)
    selected_indices = sorted(selected_indices)  # Sort for sequential reading
    
    # Second pass: write selected lines
    with open(input_path) as f_in, open(output_path, 'w') as f_out:
        for i, line in enumerate(f_in):
            if not selected_indices:  # All selected lines have been written
                break
            if i == selected_indices[0]:
                f_out.write(line)
                selected_indices.pop(0)
    
    print(f"Created sample at {output_path}")

def create_dataset():
    """Main function to create/download the dataset"""
    # Setup paths
    data_dir = Path("data/raw")
    sampled_dir = Path("data/processed")
    file_path = data_dir / "signalmedia-1m.jsonl.gz"
    uncompressed_path = data_dir / "signalmedia-1m.jsonl"
    
    # Create data directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if compressed file exists and download if needed
    if not file_path.exists():
        url = "https://research.signal-ai.com/newsir16/signalmedia-1m.jsonl.gz"
        download_file(url, file_path)
    else:
        print(f"Dataset already exists at {file_path}")
    
    # Uncompress if needed
    if not uncompressed_path.exists():
        uncompress_dataset(file_path, uncompressed_path)
    else:
        print(f"Uncompressed dataset already exists at {uncompressed_path}")
    
    #Create samples
    sample_sizes = [10, 100, 1000, 10000]
    keywords = {'legal' : ['lawyer', 'law firm', 'attorney', 'litigation', 'arbitration'],
                'vc': ['funding round', 'raising capital', 'millions raised', 'venture capital'],
                'none' : []}
    
    for size in sample_sizes:
        for filter in keywords.keys():
            sample_path = sampled_dir / f"signalmedia-{size}-{filter}.jsonl"
            if not sample_path.exists():
                create_sample(uncompressed_path, sample_path, size, keywords[filter]) # TODO: Allow create_samples in one go.
        else:
            print(f"Sample of {size} lines already exists at {sample_path}")
    
    
if __name__ == "__main__":
    create_dataset()
