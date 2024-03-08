
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
from io import StringIO, BytesIO
import boto3

load_dotenv()

class ApiLimitReached(Exception):
    pass

class Ingestion:

    def __init__(self) -> None:

        self.API_KEY = os.getenv('api_key1')
        self.AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('aws_secret_access_key')

        self.places = {'Canto das Pedras - Açu' : (  -21.84931242362662 , -40.995046258708484), 
                'Praia de Itaúna - Saquarema': (-22.935712293847164, -42.48337101071781),
                'Praia de Grussaí - São João da Barra': (-21.69321525422544, -41.02351641556605),
                'Praia de Geribá - Búzios': (-22.778805307418207 , -41.910794828528886),
                'Praia de Joaquina - Florianópolis': (-22.935712293847164, -42.48337101071781)
                }
        self.s3_client = boto3.client('s3', 
                            aws_access_key_id=self.AWS_ACCESS_KEY_ID, 
                            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)

    @staticmethod
    def calculate_mean(value) -> float:
        return sum(list(value.values()))/len(value)
    
    @staticmethod
    def degrees_to_compass(degrees) -> str:
        directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N']
        
        # Calculate index based on degree range
        index = int((degrees + 22.5) / 45)
        # Return corresponding direction
        return directions[index]
    
    def transform_data(self, df, place):
        
        df['air_temperature'] = df['airTemperature'].apply(lambda x: x['noaa'])
        df['swell_height'] = df['swellHeight'].apply(lambda x: x['noaa'])
        df['swell_period'] = df['swellPeriod'].apply(lambda x: x['noaa'])
        df['wave_height'] = df['waveHeight'].apply(lambda x: x['noaa'])
        df['wave_period'] = df['wavePeriod'].apply(lambda x: x['noaa'])
        df['wind_direction'] = df['windDirection'].apply(lambda x: Ingestion.degrees_to_compass(x['noaa']))
        df['wind_speed'] = df['windSpeed'] .apply(lambda x: x['noaa']*3.6)
        
        df['place'] = place
        df['time'] = pd.to_datetime(df['time'])
        df = df.drop(['airTemperature', 'swellHeight', 'swellPeriod', 'waveHeight', 'wavePeriod', 'airTemperature', 'windDirection', 'windSpeed' ], axis=1)
        return df
    def upload_dataframe_to_s3(self, dataframe, bucket_name, object_name):
        csv_buffer = StringIO()
        # Salva o DataFrame como um CSV em memória
        dataframe.to_csv(csv_buffer, index=False)
        # Envia o conteúdo do CSV para o S3
        self.s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_name)
        
        print("Arquivo enviado para o S3 com sucesso!")
        
    def load_data_from_s3_boto3(self, bucket_name, file_key):
        """
        Loads data from S3 using boto3 and pandas.

        Args:
            bucket_name (str): Name of the S3 bucket containing the data.
            file_key (str): Path to the file within the S3 bucket.

        Returns:
            pandas.DataFrame: The loaded DataFrame from S3.
        """

        # Create S3 client

        # Get data object from S3
        data_obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)

        # Create in-memory file object
        data_bytes = data_obj['Body'].read()
        data_file = BytesIO(data_bytes)

        # Read data using pandas
        df = pd.read_csv(data_file)

        return df

    def pipeline(self, date = None):
        
        if not date:
            start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        else:
            date = datetime.strptime(date, '%Y-%m-%d')    

            start = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end = date.replace(hour=23, minute=59, second=59, microsecond=999999)

        df = pd.DataFrame()
        for place, loc in self.places.items():
            lat = loc[0]
            lng = loc[1]
            
            response = requests.get(
            'https://api.stormglass.io/v2/weather/point',
            params={
                'lat': lat,
                'lng': lng,
                'params': ','.join(['waveHeight', 'windSpeed','wavePeriod','windDirection','swellHeight', 'swellPeriod','airTemperature']),
                'start': start.timestamp(),  # Convert to UTC timestamp
                'end': end.timestamp(),# Convert to UTC timestamp'
            },
            headers={
                'Authorization': self.API_KEY
            }
            )
            if response.status_code == 402:
                raise ApiLimitReached
            # Do something with response data.
            json_data = response.json()
            data = json_data['hours']

            request_df = pd.DataFrame(data)

            request_df = self.transform_data(request_df, place)

            df = pd.concat([df, request_df])


        bucket_name = 'surfline'
        filename = date.date().strftime('%Y-%m-%d')
        object_name = f'{filename}.csv'

        #     # Faz o upload do DataFrame para o S3
        self.upload_dataframe_to_s3(df, bucket_name, object_name)
        return 200


   
