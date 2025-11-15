import requests

response = requests.get('https://8000-cs-93f8b8c1-e284-48a7-9658-42807dcfd1e0.cs-europe-west1-xedi.cloudshell.dev/all_csv_flights/')
csv_content = response.text

import gzip
import io
import pandas as pd

gz_buffer = io.BytesIO(response.content)
with gzip.open(gz_buffer, 'rt') as f:
    df = pd.read_csv(f,low_memory=False)

print(csv_content)