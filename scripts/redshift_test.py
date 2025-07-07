import argparse
import os
import logging
import glob
import re
import psycopg2

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get Redshift connection details from environment variables
redshift_db_url = os.getenv("REDSHIFT_SERVERLESS_URL")

# If not in environment variables, try to get from .env file
if not redshift_db_url:
    env_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as env_file:
            for line in env_file:
                if line.strip().startswith('REDSHIFT_SERVERLESS_URL='):
                    redshift_db_url = line.strip().split('=', 1)[1].strip()
                    # Remove quotes if present
                    if (redshift_db_url.startswith('"') and redshift_db_url.endswith('"')) or \
                       (redshift_db_url.startswith("'") and redshift_db_url.endswith("'")):
                        redshift_db_url = redshift_db_url[1:-1]
                    break

if not redshift_db_url:
    raise EnvironmentError("REDSHIFT_SERVERLESS_URL environment variable must be set or defined in .env file")

# Function to get a database connection
def get_db_connection():
    """Get a connection to the Redshift database."""
    logger.info("Connecting to Redshift")
    
    # Parse the Redshift URL
    if redshift_db_url.startswith('redshift://'):
        # Parse URL format: redshift://username:password@host:port/dbname
        parsed_url = redshift_db_url.replace('redshift://', '')
        
        # Extract username and password
        auth_part, rest = parsed_url.split('@', 1)
        username, password = auth_part.split(':', 1)
        
        # Extract host, port, and dbname
        host_port, dbname = rest.split('/', 1)
        if ':' in host_port:
            host, port = host_port.split(':', 1)
        else:
            host = host_port
            port = '5439'  # Default Redshift port
        
        # Create connection parameters
        conn_params = {
            'host': host,
            'port': port,
            'user': username,
            'password': password,
            'dbname': dbname
        }
        
        return psycopg2.connect(**conn_params)
    else:
        # Assume it's already in the correct format for psycopg2
        return psycopg2.connect(redshift_db_url)

conn = get_db_connection()

with conn.cursor() as cursor:
    # Check the actual columns in the publisher table
    cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'publisher' 
    ORDER BY ordinal_position;
    """)
    columns = cursor.fetchall()
    print("Publisher table columns:", columns)
