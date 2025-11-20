import sys
from pathlib import Path
import requests

RAW_DIR = Path("data/raw/crime")
DATASET_URL = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"


def download_csv(url: str, output_filename: str | None = None) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if output_filename is None:
        output_filename = "crime_raw.csv"

    output_path = RAW_DIR / output_filename

    if output_path.exists():
        print(f"Le fichier existe déjà : {output_path}")
        print("Aucun téléchargement effectué.")
        return output_path

    print(f"Téléchargement depuis : {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                percent = downloaded * 100 // total_size
                print(f"\rTéléchargement : {percent}%", end="", flush=True)
            else:
                print(f"\rTéléchargement : {downloaded} octets", end="", flush=True)

    print()
    print(f"Fichier enregistré dans : {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        url = sys.argv[1]
    else:
        url = DATASET_URL

    filename = sys.argv[2] if len(sys.argv) >= 3 else None
    download_csv(url, filename)
