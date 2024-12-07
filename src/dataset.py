import gzip
import requests
from pathlib import Path

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

def create_sample(input_path: Path, output_path: Path, n_lines: int) -> None:
    """Create a sample of first n_lines from input file"""
    print(f"Creating sample of first {n_lines} lines")
    
    with open(input_path) as f_in, open(output_path, 'w') as f_out:
        for i, line in enumerate(f_in):
            if i >= n_lines:
                break
            # Remove trailing newline from last line
            if i == n_lines - 1:
                f_out.write(line.rstrip())
            else:
                f_out.write(line)
    
    print(f"Created sample at {output_path}")

def create_dataset():
    """Main function to create/download the dataset"""
    # Setup paths
    data_dir = Path("data/raw")
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
    sample_sizes = [1000, 10000, 100000]
    sample_paths = []
    
    for size in sample_sizes:
        sample_path = data_dir / f"signalmedia-{size}.jsonl"
        if not sample_path.exists():
            create_sample(uncompressed_path, sample_path, size)
        else:
            print(f"Sample of {size} lines already exists at {sample_path}")
        sample_paths.append(sample_path)
    
    return uncompressed_path, sample_paths

if __name__ == "__main__":
    create_dataset()
