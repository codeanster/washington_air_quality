import json
import psycopg2
import os
import logging
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establishes and returns a connection to the PostgreSQL database.
    
    Returns:
        conn (psycopg2.connection): The database connection object.
    
    Raises:
        Exception: If unable to connect to the database.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT']
        )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Operational error connecting to the database: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"General database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database connection: {e}")
        raise

def lambda_get_current_air_quality(event, context):
    """
    Retrieves the most recent air quality data for a specific location.

    Args:
        event (dict): The event data containing the 'location' path parameter.
        context (LambdaContext): The context in which the function is called.

    Returns:
        dict: API response with the air quality data or an error message.
    """
    location = event['pathParameters']['location']

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT report_date, air_quality_pm25, air_quality_pm10, air_quality_ozone
                    FROM air_quality
                    WHERE location = %s
                    ORDER BY report_date DESC
                    LIMIT 1
                """, (location,))
                result = cur.fetchone()

        if result:
            # Convert the datetime object to a string using isoformat
            result['report_date'] = result['report_date'].isoformat() if result['report_date'] else None
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'location': location,
                    'timestamp': result['report_date'],
                    'pm25': result['air_quality_pm25'],
                    'pm10': result['air_quality_pm10'],
                    'ozone': result['air_quality_ozone']
                })
            }
        else:
            return {'statusCode': 404, 'body': json.dumps({'error': 'No data found for this location'})}

    except psycopg2.OperationalError as e:
        logger.error(f"Database operational error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database operational error occurred'})}
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database error occurred'})}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'An unexpected error occurred'})}

def lambda_get_above_avg_locations(event, context):
    """
    Identifies locations where air quality thresholds are exceeded on the most recent date.

    Args:
        event (dict): The event data (not used in this function).
        context (LambdaContext): The context in which the function is called.

    Returns:
        dict: API response with the list of locations or an error message.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT location
                    FROM import_air_quality
                    WHERE report_date = (SELECT MAX(report_date) FROM import_air_quality)
                    AND (air_quality_pm10 > 100 
                         OR air_quality_pm25 > 100 
                         OR air_quality_ozone > 100)
                """)
                locations = cur.fetchall()

        if locations:
            location_list = [location['location'] for location in locations]
            return {'statusCode': 200, 'body': json.dumps({'locations': location_list})}
        else:
            return {'statusCode': 404, 'body': json.dumps({'message': 'No locations found where air quality exceeds thresholds on the most recent date'})}

    except psycopg2.OperationalError as e:
        logger.error(f"Database operational error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database operational error occurred'})}
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database error occurred'})}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'An unexpected error occurred'})}

def lambda_get_air_quality_trend(event, context):
    """
    Analyzes air quality trend for a specific location over a given timeframe.

    Args:
        event (dict): The event data containing 'location' and 'timeframe' parameters.
        context (LambdaContext): The context in which the function is called.

    Returns:
        dict: API response with trend data or an error message.
    """
    location = event['pathParameters']['location']
    timeframe = event['queryStringParameters'].get('timeframe', 'week')

    if timeframe not in ['week', 'month']:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid timeframe. Use "week" or "month".'})}

    try:
        start_date = datetime.now() - timedelta(days=7 if timeframe == 'week' else 30)

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Fetch the air quality data at the start and end of the timeframe
                cur.execute("""
                    WITH date_range AS (
                        SELECT 
                            MIN(report_date) AS start_date,
                            MAX(report_date) AS end_date
                        FROM air_quality
                        WHERE location = %s AND report_date >= %s
                    )
                    SELECT 
                        dr.start_date,
                        dr.end_date,
                        (SELECT air_quality_pm25 FROM air_quality
                        WHERE location = %s AND report_date = dr.start_date) AS start_pm25,
                        (SELECT air_quality_pm25 FROM air_quality
                        WHERE location = %s AND report_date = dr.end_date) AS end_pm25,
                        (SELECT air_quality_pm10 FROM air_quality
                        WHERE location = %s AND report_date = dr.start_date) AS start_pm10,
                        (SELECT air_quality_pm10 FROM air_quality
                        WHERE location = %s AND report_date = dr.end_date) AS end_pm10,
                        (SELECT air_quality_ozone FROM air_quality
                        WHERE location = %s AND report_date = dr.start_date) AS start_ozone,
                        (SELECT air_quality_ozone FROM air_quality
                        WHERE location = %s AND report_date = dr.end_date) AS end_ozone
                    FROM date_range dr
                """, (location, start_date, location, location, location, location, location, location))
                result = cur.fetchone()

        if result:
            # Calculate the percentage change for each pollutant
            def calculate_change(start, end):
                if start and end:
                    return round(((end - start) / start) * 100, 2)
                return None

            trend_data = {
                'location': location,
                'timeframe': timeframe,
                'pm25_change': calculate_change(result['start_pm25'], result['end_pm25']),
                'pm10_change': calculate_change(result['start_pm10'], result['end_pm10']),
                'ozone_change': calculate_change(result['start_ozone'], result['end_ozone']),
            }

            # Determine the overall trend direction
            trend_data['trend_direction'] = 'improving' if all(
                change is not None and change < 0 for change in trend_data.values() if isinstance(change, float)
            ) else 'worsening' if all(
                change is not None and change > 0 for change in trend_data.values() if isinstance(change, float)
            ) else 'mixed'

            return {'statusCode': 200, 'body': json.dumps(trend_data)}
        else:
            return {'statusCode': 404, 'body': json.dumps({'error': 'No data found for this location and timeframe'})}

    except psycopg2.OperationalError as e:
        logger.error(f"Database operational error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database operational error occurred'})}
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Database error occurred'})}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'An unexpected error occurred'})}
