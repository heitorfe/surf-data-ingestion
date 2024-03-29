import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
import plotly.express as px
from pytz import timezone
import boto3
import os
import io
import math
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('api_key')
AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = os.getenv('aws_secret_access_key')

places = {'Canto das Pedras - Açu' : (  -21.84931242362662 , -40.995046258708484), 
        'Praia de Itaúna - Saquarema': (-22.935712293847164, -42.48337101071781),
        'Praia de Grussaí - São João da Barra': (-21.69321525422544, -41.02351641556605),
        'Praia de Geribá - Búzios': (-22.778805307418207 , -41.910794828528886),
        'Praia de Joaquina - Florianópolis': (-22.935712293847164, -42.48337101071781)
        }

s3_client = boto3.client('s3', 
                    aws_access_key_id=AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def get_current_time() -> str:
    tz = timezone('Etc/GMT+3')  # E. South America Standard Time
    current_time = datetime.now(tz)
    return current_time

@st.cache_data
def load_data_from_s3_boto3(bucket_name, file_key):
  """
  Loads data from S3 using boto3 and pandas.

  Args:
      bucket_name (str): Name of the S3 bucket containing the data.
      file_key (str): Path to the file within the S3 bucket.

  Returns:
      pandas.DataFrame: The loaded DataFrame from S3.
  """


  # Get data object from S3
  data_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)

  # Create in-memory file object
  data_bytes = data_obj['Body'].read()
  data_file = io.BytesIO(data_bytes)

  # Read data using pandas
  df = pd.read_csv(data_file)

  return df


# Sidebar for date selection and category selection
st.sidebar.title("Filters")
# Single date picker
selected_date = st.sidebar.date_input("Select Date", date.today())


# Sample data (replace with your data source)
data = {
    "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]),
    "value": [10, 15, 20, 18, 12],
    "category": ["A", "A", "B", "B", "A"]
}
df = pd.DataFrame(data)

try:
    wind_direction = st.sidebar.checkbox('Wind Direction')

    df = load_data_from_s3_boto3('surfline', f'{selected_date}.csv')
    df['time'] = pd.to_datetime(df['time']).dt.tz_convert(None)

    #brazil time
    df['time'] = df['time'].apply(lambda x: x - timedelta(hours=3))
                                


    # Combobox for category selection
    places = df['place'].unique().tolist()
    place = st.sidebar.selectbox("Select Place", places)
    filtered_df = df[df['place'] == place]
    st.title(f'{place}')

    col1, col2, col3 = st.columns(3)
    now = datetime.now()

    if selected_date == now.date():
        growth_style = "font-size: larger; font-weight: bold;"
        
        current_time = now.replace(minute=0, second=0, microsecond=0)
        current_time_df = filtered_df[filtered_df['time']==current_time]
        current_time_wind_speed = round(current_time_df['wind_speed'].squeeze(), 1)
        current_time_swell_height = round(current_time_df['swell_height'].squeeze(),2)

        next_3_hours_df = filtered_df[(filtered_df['time']>=current_time) &\
                            (filtered_df['time']<=current_time + timedelta(hours=3))]
        next_3_hours_df_swell_height_avg = round(next_3_hours_df['swell_height'].mean(), 2)

        with col1:
            st.write("Velocidade do vento agora:")
            st.write(f"<span style='{growth_style} color: white;'>{current_time_wind_speed} km/h</span>", unsafe_allow_html=True)
        with col2:
            st.write("Altura da onda agora:")
            st.write(f"<span style='{growth_style} color: white;'>{current_time_swell_height} m</span>", unsafe_allow_html=True)
        with col3:
            st.write("Altura da onda nas próximas 3h:")
            st.write(f"<span style='{growth_style} color: white;'>{next_3_hours_df_swell_height_avg} m</span>", unsafe_allow_html=True)


    fig = px.bar(
        filtered_df if 'filtered_df' in locals() else df,  # Use filtered_df if available
        x="time",
        y="wind_speed",
        title="Wind Speed (km/h)"
    )


    if wind_direction:
    # Create annotations for each data point (assuming 'wind_direction' is present)
        for i, row in filtered_df.iterrows():  # Iterate through filtered data (or df if not filtered)
            x = row['time']
            y = row['wind_speed']
            wind_direction = row['wind_direction']
            annotation_text = f"{wind_direction}"

            # Position annotations slightly above the line
            y_offset = y * 0.1  # Adjust offset based on your needs

            annotation = dict(
                x=x,
                y=y + y_offset,
                text=annotation_text,
                showarrow=False
            )
            fig.add_annotation(annotation)

    st.plotly_chart(fig)

    # Title and chart
    fig = px.line(filtered_df, x="time", y="swell_height", title="Swell Height (m)")
    st.plotly_chart(fig)

except Exception as e:
    st.title(f'Data not Available. Please select another date')