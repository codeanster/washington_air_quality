import json
import urllib.request
import feedparser
import re
import psycopg2
import os
import boto3
from datetime import datetime
import logging
import csv

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to fetch and parse RSS feed
def fetch_rss_feed(url):
    feed = feedparser.parse(url)
    data = []

    for entry in feed.entries:
        title = entry.get('title', 'No title')
        link = entry.get('link', 'No link')
        summary = entry.get('summary', 'No summary')

        location = "No location"
        report_date = None
        air_quality_pm25 = None
        air_quality_pm10 = None
        air_quality_ozone = None
        agency = "No agency"
        last_update = None

        if 'summary_detail' in entry:
            summary_html = entry['summary_detail'].get('value', '')

            # Extract information using regex
            location_match = re.search(r'<div><b>Location:</b>\s*(.*?)</div>', summary_html)
            date_match = re.search(r'<b>Current Air Quality:</b>\s*(.*?)<br', summary_html)
            air_quality_matches = re.findall(r'(Good|Moderate|Unhealthy for Sensitive Groups|Unhealthy|Very Unhealthy|Hazardous)\s*-\s*(\d+)\s*AQI\s*-\s*([^<]+)', summary_html)
            agency_match = re.search(r'<div><b>Agency:</b>\s*(.*?)</div>', summary_html)
            last_update_match = re.search(r'<div><i>Last Update: (.*?)</i></div>', summary_html)
            unavailable_match = re.search(r'Current Air Quality unavailable for\s*(.*?)<br', summary_html)

            if unavailable_match:
                location = unavailable_match.group(1)
            else:
                if location_match:
                    location = location_match.group(1).strip()
                if date_match:
                    date_str = date_match.group(1).strip()
                    date_str = re.sub(r' [Pp][Dd][Tt]| [Pp][Ss][Tt]$', '', date_str)
                    report_date = datetime.strptime(date_str, '%m/%d/%y %I:%M %p')
                if agency_match:
                    agency = agency_match.group(1).strip()
                if last_update_match:
                    last_update_str = last_update_match.group(1).strip()
                    last_update_str = re.sub(r' [Pp][Dd][Tt]| [Pp][Ss][Tt]$', '', last_update_str)
                    last_update = datetime.strptime(last_update_str, '%a, %d %b %Y %H:%M:%S')
                if air_quality_matches:
                    for match in air_quality_matches:
                        description, aqi, pollutant = match
                        aqi = int(aqi)
                        if '2.5 microns' in pollutant:
                            air_quality_pm25 = aqi
                        elif '10 microns' in pollutant:
                            air_quality_pm10 = aqi
                        elif 'Ozone' in pollutant:
                            air_quality_ozone = aqi

        entry_data = {
            'title': title,
            'link': link,
            'location': location,
            'report_date': report_date,
            'air_quality_pm25': air_quality_pm25,
            'air_quality_pm10': air_quality_pm10,
            'air_quality_ozone': air_quality_ozone,
            'agency': agency,
            'last_update': last_update
        }
        data.append(entry_data)

    return data

# Function to read URLs from CSV file
def read_urls_from_csv():
    csv_path = os.path.join(os.path.dirname(__file__), 'city_urls.csv')
    urls = []
    with open(csv_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            urls.append(row['URL'])
    return urls

# Function to get database connection
def get_db_connection():
    ssm = boto3.client('ssm')
    db_params = ssm.get_parameters(
        Names=['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT'],
        WithDecryption=True
    )
    params = {param['Name']: param['Value'] for param in db_params['Parameters']}
    
    conn = psycopg2.connect(
        dbname=params['DB_NAME'],
        user=params['DB_USER'],
        password=params['DB_PASSWORD'],
        host=params['DB_HOST'],
        port=params['DB_PORT']
    )
    return conn

# Function to save data to database
def save_to_db(data, conn):
    cur = conn.cursor()
    for entry in data:
        try:
            cur.execute("""
                INSERT INTO air_quality (
                    title,
                    link,
                    location,
                    report_date,
                    air_quality_pm25,
                    air_quality_pm10,
                    air_quality_ozone,
                    agency,
                    last_update
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (location, report_date) DO NOTHING
                """, (
                    entry['title'],
                    entry['link'],
                    entry['location'].split(',')[0].strip(),
                    entry['report_date'],
                    entry['air_quality_pm25'],
                    entry['air_quality_pm10'],
                    entry['air_quality_ozone'],
                    entry['agency'],
                    entry['last_update']
                )
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to insert entry {entry}: {e}")
            conn.rollback()
    cur.close()

# Lambda handler function
def lambda_handler(event, context):
    try:
        urls = read_urls_from_csv()
        
        conn = get_db_connection()
        
        for url in urls:
            logger.info(f"Processing URL: {url}")
            data = fetch_rss_feed(url)
            save_to_db(data, conn)
        
        conn.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data collection and storage completed successfully')
        }
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error in data collection: {str(e)}')
        }