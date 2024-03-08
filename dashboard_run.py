import datetime
import pytz  # Library for handling timezones
import subprocess

# Define Brazil timezone
brazil_tz = pytz.timezone('America/Sao_Paulo')

def is_time_window():
    """
    Checks if the current time in Brazil timezone is between 4:00 and 15:00.
    """
    now = datetime.datetime.now(brazil_tz)
    start_time = datetime.time(hour=4)
    end_time = datetime.time(hour=20)
    return start_time <= now.time() < end_time

if is_time_window():
    subprocess.run(['streamlit', 'run', 'dashboard.py'])
    print("Streamlit app started within the time window (Brazil time).")
else:
    # Optional: Terminate Streamlit process (implementation depends on your setup)
    subprocess.run(['pkill', '-f', 'streamlit'])  # Example using pkill (careful!)
    print("Streamlit app terminated outside the time window (Brazil time).")