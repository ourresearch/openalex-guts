 #!/usr/bin/env python3
"""
Redshift Materialized View Modification Script
---------------------------------------------

This script modifies or refreshes a materialized view in Amazon Redshift Serverless. It can also
identify and recreate/refresh all dependent materialized views in the correct order.

Usage:
    python3 modify_redshift_mv.py <mv_name> [--refresh [all]]
    python3 modify_redshift_mv.py --all

Example:
    python3 modify_redshift_mv.py affiliation_mv
    python3 modify_redshift_mv.py institution_mv
    python3 modify_redshift_mv.py affiliation_mv --refresh
    python3 modify_redshift_mv.py --refresh all
    python3 modify_redshift_mv.py --all

Requirements:
    - Redshift connection URL (in REDSHIFT_SERVERLESS_URL environment variable or .env file)

Notes:
    - The script will automatically find and recreate all dependent materialized views
    - Dependencies are determined by scanning SQL files in the /sql/redshift/ directory
    - Transitive dependencies are handled correctly (if A depends on B and B depends on C,
      then modifying C will recreate both B and A)
    - A single database connection is used for the entire operation
"""
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

def build_dependency_graph():
    """
    Build a dependency graph for all materialized views.
    
    Returns:
        dict: A dictionary where keys are MV names and values are lists of dependencies
    """
    # Get all materialized views
    all_mvs = get_all_materialized_views()
    
    # Build a complete dependency graph
    dependency_graph = {}
    
    # Path to SQL files directory
    sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sql', 'redshift')
    
    # Get all SQL files
    sql_files = glob.glob(os.path.join(sql_dir, "*.sql"))
    
    # Build the dependency graph
    for sql_file in sql_files:
        file_mv_name = os.path.basename(sql_file).replace('.sql', '')
        dependency_graph[file_mv_name] = []
        
        # Read the SQL file content
        with open(sql_file, 'r') as f:
            content = f.read()
        
        # Check for dependencies in the content
        for other_sql_file in sql_files:
            other_mv_name = os.path.basename(other_sql_file).replace('.sql', '')
            if other_mv_name != file_mv_name:
                # Pattern to match the MV name in SQL files
                pattern = r'\b' + re.escape(other_mv_name) + r'\b'
                if re.search(pattern, content):
                    dependency_graph[file_mv_name].append(other_mv_name)
    
    return dependency_graph

def topological_sort(graph):
    """
    Perform topological sort on the dependency graph.
    This ensures that a view is only processed after all its dependencies.
    
    Args:
        graph (dict): Dependency graph where keys are nodes and values are lists of dependencies
        
    Returns:
        list: Topologically sorted list of nodes
    """
    # Track visited nodes and result
    visited = set()
    temp_visited = set()  # For cycle detection
    result = []
    
    def visit(node):
        if node in temp_visited:
            # Cycle detected, which shouldn't happen with MVs
            logger.warning(f"Cycle detected in dependency graph involving {node}")
            return
        
        if node not in visited:
            temp_visited.add(node)
            
            # Visit all dependencies first
            for dep in graph.get(node, []):
                visit(dep)
            
            temp_visited.remove(node)
            visited.add(node)
            result.append(node)
    
    # Visit all nodes
    for node in graph:
        if node not in visited:
            visit(node)
    
    return result


def find_mv_dependencies(mv_name):
    """
    Find all materialized views that depend on the given materialized view,
    including transitive dependencies.
    
    Args:
        mv_name (str): Name of the materialized view to check dependencies for
    
    Returns:
        list: List of materialized view names that depend on the given MV, ordered by dependency
    """
    logger.info(f"Finding dependencies for materialized view: {mv_name}")
    
    dependency_graph = build_dependency_graph()
    
    # Function to find all dependencies recursively
    def find_all_dependencies(mv, visited=None, all_deps=None):
        if visited is None:
            visited = set()
        if all_deps is None:
            all_deps = []
        
        if mv in visited:
            return all_deps
        
        visited.add(mv)
        
        for dep in dependency_graph.get(mv, []):
            if dep not in all_deps:
                all_deps.append(dep)
            find_all_dependencies(dep, visited, all_deps)
        
        return all_deps
    
    # Find all MVs that depend on our target MV
    dependent_mvs = []
    for mv in dependency_graph:
        if mv_name in find_all_dependencies(mv):
            dependent_mvs.append(mv)
            logger.info(f"Found dependent MV: {mv}")
    
    # Topological sort to ensure correct dependency order
    sorted_mvs = topological_sort({mv: dependency_graph.get(mv, []) for mv in dependent_mvs})
    
    # Filter to only include dependent MVs (not their dependencies)
    # and exclude the target MV itself
    sorted_dependent_mvs = [mv for mv in sorted_mvs if mv in dependent_mvs and mv != mv_name]
    
    return sorted_dependent_mvs


def get_mv_sql(mv_name):
    """
    Get the SQL definition for the given materialized view.
    
    Args:
        mv_name (str): Name of the materialized view
    
    Returns:
        str: SQL definition of the materialized view
    """    
    # Path to SQL file
    sql_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
        'sql', 'redshift', f"{mv_name}.sql"
    )
    
    # Check if SQL file exists
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"SQL file not found for materialized view: {mv_name}")
    
    # Read the SQL file content
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    return sql


def execute_sql(conn, sql):
    """
    Execute SQL on Redshift.
    
    Args:
        conn: Database connection
        sql (str): SQL to execute
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing SQL: {e}")
        raise


def drop_mv_with_cascade(conn, mv_name):
    """
    Drop the materialized view with CASCADE option.
    
    Args:
        conn: Database connection
        mv_name (str): Name of the materialized view to drop
    """
    logger.info(f"Dropping materialized view with CASCADE: {mv_name}")    
    drop_sql = f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;"
    execute_sql(conn, drop_sql)
    logger.info(f"Successfully dropped materialized view: {mv_name}")


def create_mv(conn, mv_name):
    """
    Create the materialized view using its SQL definition.
    
    Args:
        conn: Database connection
        mv_name (str): Name of the materialized view to create
    """
    logger.info(f"Creating materialized view: {mv_name}")
    sql = get_mv_sql(mv_name)
    execute_sql(conn, sql)
    logger.info(f"Successfully created materialized view: {mv_name}")


def modify_mv(conn, mv_name):
    """
    Modify the materialized view by dropping it with CASCADE and recreating it.
    Also recreates all dependent materialized views in the correct order.
    
    Args:
        conn: Database connection
        mv_name (str): Name of the materialized view to modify
    """
    logger.info(f"Starting modification of materialized view: {mv_name}")
    dependent_mvs = find_mv_dependencies(mv_name)
    logger.info(f"Found {len(dependent_mvs)} dependent materialized views: {dependent_mvs}")
    
    drop_mv_with_cascade(conn, mv_name)
    create_mv(conn, mv_name)
    logger.info(f"Successfully modified materialized view: {mv_name}")
    
    # Recreate dependent materialized views in the correct order
    for dep_mv in dependent_mvs:
        create_mv(conn, dep_mv)
    
    logger.info(f"Successfully recreated all dependent materialized views")


def get_all_materialized_views():
    """
    Get a list of all materialized views in the SQL directory.
    
    Returns:
        list: List of all materialized view names
    """
    sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sql', 'redshift')
    sql_files = glob.glob(os.path.join(sql_dir, "*.sql"))
    mv_names = [os.path.basename(sql_file).replace('.sql', '') for sql_file in sql_files]
    
    return mv_names


def refresh_mv_in_db(conn, mv_name):
    """
    Refresh a materialized view in the database.
    
    Args:
        conn: Database connection
        mv_name (str): Name of the materialized view to refresh
    """
    logger.info(f"Refreshing materialized view: {mv_name}")
    sql = f"REFRESH MATERIALIZED VIEW {mv_name};"
    execute_sql(conn, sql)
    logger.info(f"Successfully refreshed materialized view: {mv_name}")


def refresh_all_mvs(conn):
    """
    Refresh all materialized views in the correct dependency order.
    
    Args:
        conn: Database connection
    """
    logger.info("Starting refresh of all materialized views")
    dependency_graph = build_dependency_graph()
    sorted_mvs = topological_sort(dependency_graph)
    
    # Refresh all materialized views in the correct order
    for mv_name in sorted_mvs:
        refresh_mv_in_db(conn, mv_name)
    
    logger.info("Successfully refreshed all materialized views")


def refresh_mv(conn, mv_name):
    """
    Refresh a materialized view and all its dependencies.
    
    Args:
        conn: Database connection
        mv_name (str): Name of the materialized view to refresh
    """
    logger.info(f"Starting refresh of materialized view: {mv_name} and its dependencies")
    dependent_mvs = find_mv_dependencies(mv_name)
    logger.info(f"Found {len(dependent_mvs)} dependent materialized views: {dependent_mvs}")
    
    # Refresh the specified materialized view
    refresh_mv_in_db(conn, mv_name)
    
    # Refresh dependent materialized views in the correct order
    for dep_mv in dependent_mvs:
        refresh_mv_in_db(conn, dep_mv)
    
    logger.info(f"Successfully refreshed {mv_name} and all its dependent materialized views")


def create_all_mvs(conn):
    """
    Create all materialized views in the correct dependency order.
    This function first builds a dependency graph and then creates MVs in topological order.
    
    Args:
        conn: Database connection
    """
    logger.info("Starting creation of all materialized views")
    dependency_graph = build_dependency_graph()
    sorted_mvs = topological_sort(dependency_graph)
    logger.info(f"Creating materialized views in this order: {sorted_mvs}")
    
    # First drop all existing materialized views to avoid conflicts
    for mv_name in reversed(sorted_mvs):
        try:
            drop_mv_with_cascade(conn, mv_name)
        except Exception as e:
            logger.warning(f"Error dropping {mv_name}: {e}")
    
    # Create all materialized views in the correct order
    for mv_name in sorted_mvs:
        try:
            create_mv(conn, mv_name)
        except Exception as e:
            logger.error(f"Error creating {mv_name}: {e}")
            raise
    
    logger.info("Successfully created all materialized views")


def main():
    """Main function to parse arguments and modify or refresh the materialized view."""
    parser = argparse.ArgumentParser(description="Modify or refresh a materialized view in Amazon Redshift Serverless.")
    parser.add_argument("mv_name", nargs='?', help="Name of the materialized view to modify or refresh")
    parser.add_argument("--refresh", nargs='?', const=True, help="Refresh the materialized view instead of modifying it. Use '--refresh all' to refresh all views.")
    parser.add_argument("--all", action="store_true", help="Create all materialized views in the proper dependency order.")
    args = parser.parse_args()
    
    # Create a single connection for the entire operation
    conn = get_db_connection()
    try:
        if args.all:
            # Create all materialized views
            create_all_mvs(conn)
        elif args.refresh == 'all':
            # Refresh all materialized views
            refresh_all_mvs(conn)
        elif args.refresh:
            # Refresh the specified materialized view and its dependencies
            if not args.mv_name:
                parser.error("MV name is required when using --refresh without 'all'")
            refresh_mv(conn, args.mv_name)
        else:
            # Modify the materialized view
            if not args.mv_name:
                parser.error("MV name is required when not using --refresh all or --all")
            modify_mv(conn, args.mv_name)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()