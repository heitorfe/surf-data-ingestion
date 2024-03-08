from ingestion import Ingestion, ApiLimitReached
from datetime import datetime, timedelta
import os

today = datetime.now().strftime('%Y-%m-%d')
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

dates = [today, tomorrow]
ingestion = Ingestion()

for date in dates:

    try:
        ingestion.pipeline(date)

    except ApiLimitReached:
        print('limite atingido')
        ingestion.API_KEY = os.getenv('api_key2')
        ingestion.pipeline(date)

