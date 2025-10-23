import requests
import psycopg2




def get_weather_data():
    api_key = "b1M1nFjmH2yLXMwaUj5pxwp2XqTW3wnA"
    city = "bengaluru"
    url = f"https://api.tomorrow.io/v4/weather/realtime?location={city}&apikey={api_key}"


    headers = {
        "accept": "application/json",
        "accept-encoding": "deflate, gzip, br"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
        data = response.json()['data']
        location =  response.json()['location']
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


    output = {}
    output['time'] = data['time']
    output['humidity'] = data['values']['humidity']
    output['precipitationProbability'] = data['values']['precipitationProbability']
    output['pressureSeaLevel'] = data['values']['pressureSeaLevel']
    output['pressureSurfaceLevel'] = data['values']['pressureSurfaceLevel']
    output['rainIntensity'] = data['values']['rainIntensity']
    output['temperature'] = data['values']['temperature']
    output['city'] = location['name']

    return output


# create db connetion 

def get_db_connection():
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow", 
            port=5432
        )
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise e

    return conn

def create_db_(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP,
        humidity INTEGER,
        precipitation_probability INTEGER,
        pressure_sea_level FLOAT,
        pressure_surface_level FLOAT,
        rain_intensity FLOAT,
        temperature FLOAT,
        city VARCHAR(255)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("Table created successfully or already exists.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise e

def insert_weather_data(conn, weather_data):
    insert_query = """
    INSERT INTO weather_data (time, humidity, precipitation_probability, pressure_sea_level, pressure_surface_level, rain_intensity, temperature, city)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        cursor = conn.cursor()
        cursor.execute(insert_query, (
            weather_data['time'],
            weather_data['humidity'],
            weather_data['precipitationProbability'],
            weather_data['pressureSeaLevel'],
            weather_data['pressureSurfaceLevel'],
            weather_data['rainIntensity'],
            weather_data['temperature'],
            weather_data['city']
        ))
        conn.commit()
        cursor.close()
        print("Weather data inserted successfully.")
    except psycopg2.Error as e:
        print(f"Error inserting weather data: {e}")
        raise e


def main():
    try:
        conn = get_db_connection()
        create_db_(conn)
        insert_weather_data(conn, get_weather_data())
        # conn.close()
    except Exception as e:  
        print(f"An error occurred: {e}")
        raise e
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
