import requests

response = requests.get('https://fastapi-mongo-db-app-901450845940.europe-west1.run.app/all_csv_flights/')
csv_content = response.text

import gzip
import io
import pandas as pd

gz_buffer = io.BytesIO(response.content)
with gzip.open(gz_buffer, 'rt') as f:
    df = pd.read_csv(f,low_memory=False)

print(df)