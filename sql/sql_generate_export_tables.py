import argparse
import logging
from os import path
from os import getenv
import re

GENERATE_CREATE_TABLE = False
GENERATE_COMMENTS = False
GENERATE_UNLOAD = True
GENERATE_COPY = False

DUMP_DIR = "2022-04-07"


##  python -m sql.sql_generate_export_tables  -i sql/export_views.sql -o sql/export_tables_generated.sql

lookup_export_filenames = {
    # 'outs.EntityRelatedEntities':       'advanced/EntityRelatedEntities',
    # 'outs.FieldOfStudyChildren':        'advanced/FieldOfStudyChildren',
    # 'outs.FieldOfStudyExtendedAttributes': 'advanced/FieldOfStudyExtendedAttributes',
    'outs.FieldsOfStudy':               'advanced/FieldsOfStudy',
    'outs.PaperFieldsOfStudy':          'advanced/PaperFieldsOfStudy',
    'outs.PaperMeSH':                   'advanced/PaperMeSH',
    'outs.PaperRecommendations':        'advanced/PaperRecommendations',
    # 'outs.RelatedFieldOfStudy':         'advanced/RelatedFieldOfStudy',
    'outs.Affiliations':                'mag/Affiliations',
    'outs.AuthorExtendedAttributes':    'mag/AuthorExtendedAttributes',
    'outs.Authors':                     'mag/Authors',
    # 'outs.ConferenceInstances':         'mag/ConferenceInstances',
    # 'outs.ConferenceSeries':            'mag/ConferenceSeries',
    'outs.Journals':                    'mag/Journals',
    'outs.PaperAuthorAffiliations':     'mag/PaperAuthorAffiliations',
    'outs.PaperExtendedAttributes':     'mag/PaperExtendedAttributes',
    'outs.PaperReferences':             'mag/PaperReferences',
    'outs.PaperUrls':                   'mag/PaperUrls',
    # 'outs.PaperResources':              'mag/PaperResources',
    'outs.Papers':                      'mag/Papers',
    'outs.PaperAbstractsInvertedIndex': 'nlp/PaperAbstractsInvertedIndex',
    # 'outs.PaperCitationContexts':       'nlp/PaperCitationContexts'
}


####################################################################################################
# Define name of the tool                                                                          #
####################################################################################################

PROGRAM_NAME = 'SQL_PARSER'

####################################################################################################
# Define regexp filters and groups                                                                 #
####################################################################################################

VIEW_START_REGEXP = r'create\s*(or\s*replace)?\s*view'
VIEW_END_REGEXP = r'.*;\s*$'
VIEW_TABLENAME_REGEXP = r'create\s*(or\s*replace)?\s*view\s*([\w."]*)'
REG_GROUP_TABLE_NAME = 2
VIEW_COMMENT_REGEXP = r'create\s*(or\s*replace)?\s*view\s*([\w."]*)\s*---*\s*((\s*[\w.,"()/;:]*)*)'
REG_GROUP_COMMENT = 3
VIEW_DIST_CONFIG_REGEXP = r'--*\s*(DISTSTYLE|DISTKEY|SORTKEY)'
VIEW_QUERY_START_REGEXP = r'^\s*as\s*\(\s*$'
VIEW_QUERY_END_REGEXP = r'^\s*from\s'
VIEW_QUERY_WITH_REGEXP = r'\s*with\s'
VIEW_QUERY_COMMA_REGEXP = r'^.*,$'
VIEW_QUERY_COMMENTED_OUT_REGEXP = r'^\s*---*.*$'
VIEW_CONTAINS_COMMENT = r'^\s*[\w.]+.*---*\s*(.*)'
REG_COLUMN_COMMENT = 1
VIEW_COLUMN_CONTAINS_AS = r'^[^-]* AS\s*([\w."]*)'
REG_COLUMN_CONTAINS_AS_GROUP = 1
VIEW_COLUMN_NAME = r'^\s*([\w."]*)[,|\s]'
REG_COLUMN_NAME_GROUP = 1


####################################################################################################
# Class VIEW - For parsing and storing all view related data                                        #
####################################################################################################

# parse view name from header
def parse_view_name(raw_header):
    # look for view name
    if re.match(f'{VIEW_START_REGEXP}', raw_header, re.IGNORECASE):
        # double check if we can parse it
        if not re.search(f'{VIEW_TABLENAME_REGEXP}', raw_header, re.IGNORECASE).group(REG_GROUP_TABLE_NAME):
            # if parsing fails, then log err and raise it
            logging.error(f'Cannot parse table_name out for following query: {raw_header}')
            raise ValueError(f'Cannot parse table_name out for following query: {raw_header}')
        else:
            # in case of view_name retrieval, return it out of function
            return re.search(f'{VIEW_TABLENAME_REGEXP}', raw_header, re.IGNORECASE).group(REG_GROUP_TABLE_NAME)

# ---------------------------------------------------------------------------------------------------

# parse view comment from header
def parse_view_comment(raw_header):
    # check if header has view comment inside
    if re.match(f'{VIEW_TABLENAME_REGEXP}', raw_header, re.IGNORECASE):
        # if comment retrieval failed, then logg err and raise it
        if not re.search(f'{VIEW_COMMENT_REGEXP}', raw_header, re.IGNORECASE).group(REG_GROUP_COMMENT):
            logging.error(f'Cannot parse comment out for following query: {raw_header}')
            raise ValueError(f'Cannot parse comment out for following query: {raw_header}')
        # if comment is there then return it
        else:
            return re.search(f'{VIEW_COMMENT_REGEXP}', raw_header, re.IGNORECASE).group(REG_GROUP_COMMENT)

# ---------------------------------------------------------------------------------------------------

# class definition for view - For parsing and storing all view related data
class view:
    # Constructor of view (this is executed when view object is created)
    def __init__(self, raw_header, raw_body, view_number):
        # log start and save basic variable passed by function
        logging.info(f'Starting creation of view with object number: \'{view_number}\'')
        self.raw_header = raw_header
        # parse view name and log it
        self.view_name = parse_view_name(raw_header)
        logging.info(
            f'View name \'{self.view_name}\' extracted for following header: \'{raw_header}\' (view with object number: \'{view_number}\')')
        # parse view comment and log it
        self.view_comment = parse_view_comment(raw_header)
        logging.info(
            f'View comment \'{self.view_comment}\' extracted for following header: \'{raw_header}\' (view with object number: \'{view_number}\')')
        # parse view distribution config and log it
        self.view_dist_config = self.parse_dist_config(raw_body)
        # format distribution config  (just for logging purpose)
        view_dist_config_formated = self.view_dist_config.replace('\n', ' \\n ')
        logging.info(
            f'View Distribution config\'{view_dist_config_formated}\' extracted from following view: \'{self.view_name}\' (view with object number: \'{view_number}\')')
        # parse view query and dont log it ( its too big to log)
        self.view_query = self.parse_view_query(raw_body)
        # parse comment + column name and log them
        self.comments, self.view_columns = self.parse_view_columns(self.view_query)
        for i in range(self.comments.__len__()):
            logging.info(
                f'View column comment: \'{self.comments[i]}\' extracted for column: \'{self.view_columns[i]}\' (view with object number: \'{view_number}\')')
        logging.info(f'Creation of view with object number: \'{view_number}\' finished')

# ---------------------------------------------------------------------------------------------------
    # Method for generating table script based on view object
    def generate_table(self):
        table_name = re.sub(r'_view', '', self.view_name)

        # Start with DROP table ... header
        result = ""

        if GENERATE_CREATE_TABLE:
            result += f'DROP TABLE IF EXISTS {table_name};\n'

            # Then with CREATE table ... header
            result += f'CREATE TABLE {table_name}\n'
            # Add distribution config
            result += self.view_dist_config
            # define source select
            result += f'as (SELECT * FROM {self.view_name});\n'
            result += '\n\n'

        # If there is comment on table then add it
        if GENERATE_COMMENTS:
            if self.view_name:
                result += f'COMMENT ON TABLE {table_name} IS \'{self.view_comment}\';\n'
            # If there is not comment on table then warn user
            else:
                logging.warn(
                    f'Table \'{self.table_name}\' Is missing table description!')
            # Iterate all columns and comment to generate DDL command for them
            for i in range(self.comments.__len__()):
                # column_name_no_quotes = self.view_columns[i].replace('"', '')
                # result += f'ALTER TABLE {table_name} ALTER COLUMN RENAME {column_name_no_quotes} TO {self.view_columns[i]};\n'
                if self.comments[i]:
                    result += f'COMMENT ON COLUMN {table_name}.{self.view_columns[i]} IS \'{self.comments[i]}\';\n'
        return result

# ---------------------------------------------------------------------------------------------------
    # Method for generating more commands
    def generate_more_commands(self):
        table_name = re.sub(r'_view', '', self.view_name)
        export_dir = lookup_export_filenames.get(table_name.replace('"', ''), None)
        result = ""
        if not export_dir:
            return result

        export_file_name = export_dir.split("/")[1]
        aws_access_key_id = getenv("AWS_ACCESS_KEY_ID_OPENALEX_OPEN_DATA")
        aws_secret_access_key = getenv("AWS_SECRET_ACCESS_KEY_OPENALEX_OPEN_DATA")

        # Start with UNLOAD table ... header

        if GENERATE_UNLOAD:
            view_name = self.view_name
            if "PaperAbstractsInvertedIndex" in view_name:
                result += f"""
SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAbstractsInvertedIndex_view" where false', 
   aws_commons.create_s3_uri('openalex-mag-format', 'data_dump_v1/{DUMP_DIR}/nlp/PaperAbstractsInvertedIndex.txt.', 'us-east-1'),
   options :='format csv, delimiter E''\\t'', HEADER true, ESCAPE ''\\'', NULL '''''
);"""
                result += f"""
SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAbstractsInvertedIndex_view"', 
   aws_commons.create_s3_uri('openalex-mag-format', 'data_dump_v1/{DUMP_DIR}/nlp/PaperAbstractsInvertedIndex.txt.', 'us-east-1'),
   options :='NULL '''''
);"""

            else:
                # header file so no data
                result += f"""
SELECT * from aws_s3.query_export_to_s3('select * from {view_name} where false', 
   aws_commons.create_s3_uri('openalex-mag-format', 'data_dump_v1/{DUMP_DIR}/{export_dir}/HEADER_{export_file_name}.txt', 'us-east-1'),
   options :='format csv, delimiter E''\\t'', HEADER true, ESCAPE ''\\'', NULL '''''
);
"""
                result += "\n"
                # data
                result += f"""
SELECT * from aws_s3.query_export_to_s3('select * from {view_name}', 
   aws_commons.create_s3_uri('openalex-mag-format', 'data_dump_v1/{DUMP_DIR}/{export_dir}/{export_file_name}.txt', 'us-east-1'),
   options :=' NULL '''' '
);
"""
            result += "\n\n"

        if GENERATE_COPY:
            result += f"""
COPY {table_name}
FROM 's3://openalex-mag-format/data_dump_v1/{DUMP_DIR}/{export_file_name}.txt'
ACCESS_KEY_ID '{aws_access_key_id}' SECRET_ACCESS_KEY '{aws_secret_access_key}'
COMPUPDATE ON
ESCAPE
NULL AS ''
DELIMITER as '\\t';"""
            result += "\n\n"

        return result

# ---------------------------------------------------------------------------------------------------
    # Method for parsing our distribution config
    def parse_dist_config(self, raw_body):
        dist_config = ''
        # Go line by line
        for line in raw_body.splitlines():
            # check if current line has distribution config inside
            if re.match(f'{VIEW_DIST_CONFIG_REGEXP}', line, re.IGNORECASE):
                dist_config += re.sub(r'\s*--*\s*', '', line) + '\n'
            # If there is no distribution config, the jump out of function
            else:
                return dist_config
        return dist_config

# ---------------------------------------------------------------------------------------------------
    # Method for extracting view query (just select .. from, the rest it trimmed out here)
    def parse_view_query(self, raw_body):
        inside_view_query = False
        inside_with_clause = False
        result = ''
        # lets traver full body of view  (without header)
        for line in raw_body.splitlines():
            if not inside_view_query:
                # If not in query already, then go until you find start of query (it start with 'AS ('  command)
                if re.match(f'{VIEW_QUERY_START_REGEXP}', line, re.IGNORECASE):
                    inside_view_query = True
                # If already in query
            else:
                # Lets check if select parts ends on this row (check for from clause)
                if re.match(f'{VIEW_QUERY_END_REGEXP}', line, re.IGNORECASE):
                    return result
                # Check for 'WITH' statements that does not end on this line
                elif re.match(f'{VIEW_QUERY_WITH_REGEXP}', line, re.IGNORECASE):
                    if re.match(f'{VIEW_QUERY_COMMA_REGEXP}', line, re.IGNORECASE):
                        inside_with_clause = True
                # you are still in 'WITH' statement, check if 'WITh statement ends here'
                elif inside_with_clause:
                    inside_with_clause = False
                    if re.match(f'{VIEW_QUERY_COMMA_REGEXP}', line, re.IGNORECASE):
                        inside_with_clause = True
                # We are not in WITH statement
                else:
                    # Check if current row isn't commented out
                    if not re.match(f'{VIEW_QUERY_COMMENTED_OUT_REGEXP}', line, re.IGNORECASE):
                        result += line + '\n'

# ---------------------------------------------------------------------------------------------------
    # Method for parsing columns and comments our if view query
    # input is view query (just select .. from, the rest it trimmed out)
    def parse_view_columns(self, view_query):
        # Prepare variables
        first_line = True
        columns = []
        comments = []
        # Lets traverse view query line by line
        for line in view_query.splitlines():
            # skip first line, since that's a header and header is parsed elsewhere
            if first_line:
                first_line = False
            else:
                # Take only rows that have comments on columns. Other rows are not important
                if re.search(f'{VIEW_CONTAINS_COMMENT}', line, re.IGNORECASE):
                    if re.search(f'{VIEW_CONTAINS_COMMENT}', line, re.IGNORECASE).group(REG_COLUMN_COMMENT):
                        # We parsed out comment for column on current row. Lets save it
                        comments.append(
                            re.search(f'{VIEW_CONTAINS_COMMENT}', line, re.IGNORECASE).group(REG_COLUMN_COMMENT))
                        # After parsing column, lets check if columns is renamed (using AS notation)
                        if re.search(f'{VIEW_COLUMN_CONTAINS_AS}', line, re.IGNORECASE):
                            # Now that we know that column is renamed, lets double check and extract it
                            if re.search(f'{VIEW_COLUMN_CONTAINS_AS}', line, re.IGNORECASE).group(
                                    REG_COLUMN_CONTAINS_AS_GROUP):
                                # Now that we know that column is renamed, lets double check and extract it
                                columns.append(re.search(f'{VIEW_COLUMN_CONTAINS_AS}', line, re.IGNORECASE).group(
                                    REG_COLUMN_CONTAINS_AS_GROUP))
                            # if extraction of column name failed, then log error and raise it
                            else:
                                logging.error(f'Cannot parse comment for following row: {line}')
                                raise ValueError(f'Cannot parse comment for following row: {line}')
                        # If column isn't renamed (using AS notation), then lets just take normal column name
                        else:
                            # Lets double check that extraction of column name won't fail
                            if re.search(f'{VIEW_COLUMN_NAME}', line, re.IGNORECASE):
                                # Lets double check that extraction of column name won't fail
                                if re.search(f'{VIEW_COLUMN_NAME}', line, re.IGNORECASE).group(REG_COLUMN_NAME_GROUP):
                                    columns.append(re.search(f'{VIEW_COLUMN_NAME}', line, re.IGNORECASE).group(
                                        REG_COLUMN_NAME_GROUP))
                                # if extraction of column name failed, then log error and raise it
                                else:
                                    logging.error(f'Cannot parse comment for following row: {line}')
                                    raise ValueError(f'Cannot parse comment for following row: {line}')
                    # if extraction of column name failed, then log error and raise it
                    else:
                        logging.error(f'Cannot parse comment for following row: {line}')
                        raise ValueError(f'Cannot parse comment for following row: {line}')

                # else doesn't contain comment, but let's save it anyway
                else:
                    column_as_match = re.search(f'{VIEW_COLUMN_CONTAINS_AS}', line, re.IGNORECASE)
                    if column_as_match and column_as_match.group(REG_COLUMN_CONTAINS_AS_GROUP):
                        # Now that we know that column is renamed, lets double check and extract it
                        columns.append(column_as_match.group(REG_COLUMN_CONTAINS_AS_GROUP))
                        comments.append("")
                    # if extraction of column name failed, then log error and raise it
                    else:
                        logging.error(f'Cannot parse column for following row: {line}')
                        # raise ValueError(f'Cannot parse column for following row: {line}')

        # After extraction of all columns and all comments, lets check that their counts match
        if (columns.__len__() != comments.__len__()):
            # if counts of columns and comments does not match, then log err and raise it
            logging.error(
                f'Number of comments ({comments.__len__()}) is different then number of columns ({columns.__len__()})')
            raise ValueError(
                f'Number of comments ({comments.__len__()}) is different then number of columns ({columns.__len__()})')
        return comments, columns;


####################################################################################################
# Class parser - For parsing data on file level                                                    #
####################################################################################################

class parser:
    # parser constructor (executed during creation of parser object)
    def __init__(self):
        logging.info(f'Preparing parser to parse the SQL file')
        self.input_file_path = args['input']
        self.output_file_path = args['output']
        self.check_input()
        self.load_input_file()
        self.views = []
        logging.info(f'Parser preparation finished')

# ---------------------------------------------------------------------------------------------------

    # Method for executing parser (it does parse views and print them to file)
    def run(self):
        logging.info(f'Execution of parser started')
        self.parse_views()
        self.print_tables()
        logging.info(f'Execution of parser Finished !')

# ---------------------------------------------------------------------------------------------------

    # Method for printing tables to output file
    def print_tables(self):
        # Try printed generated code to output file
        logging.info(f'Output file generation {self.output_file_path} Started')
        try:
            f = open(self.output_file_path, "w")
            f.write('\n')
            # f.write('SET enable_case_sensitive_identifier=true;\n\n')

            # Generate table for each view object that we have
            for view in self.views:
                f.write(view.generate_table())
                f.write('\n\n')

            for view in self.views:
                f.write(view.generate_more_commands())

            if GENERATE_UNLOAD:
                aws_access_key_id = getenv("AWS_ACCESS_KEY_ID_OPENALEX_OPEN_DATA")
                aws_secret_access_key = getenv("AWS_SECRET_ACCESS_KEY_OPENALEX_OPEN_DATA")

#                 f.write(f"""\n\n
# unload ('select ''table'', ''num_rows'' as num_rows, ''size_in_mb'', ''date''
# union
# select table_name::varchar(35), num_rows::varchar(25) as num_rows, used_mb::varchar(25), sysdate::varchar(25) from v_display_table_size_and_rows order by num_rows desc')
# TO 's3://openalex-mag-format/data_dump_v1/{DUMP_DIR}/README.txt'
# ACCESS_KEY_ID '{aws_access_key_id}' SECRET_ACCESS_KEY '{aws_secret_access_key}'
# fixedwidth '0:35,1:25,2:25,3:25'
# ALLOWOVERWRITE
# parallel off; \n\n
# """)

            f.close()
        # If output file generation failed, then log error and raise it
        except:
            logging.error(f'Cannot generate output file {self.output_file_path} !')
            raise ValueError(f'Cannot generate output file {self.output_file_path} !')
        logging.info(f'Output file generation {self.output_file_path} finished !')

# ---------------------------------------------------------------------------------------------------

    def parse_views (self):
        # log start of scanning
        logging.info(f'Scan for views in input file started')
        # set initial status -> no views found, current line is not inside view code, create empty tmp variables
        scanning_status = {'inside_view': False, 'view_raw_str_tmp': '', 'view_header_str_tmp': '', 'views_found': 0}
        for line in self.raw_input_data.splitlines():
            # If last line wasn't inside view definition
            if not scanning_status['inside_view']:
                # If last line wasn't inside view definition, then check if current line is
                if re.match(f'{VIEW_START_REGEXP}', line, re.IGNORECASE):
                    scanning_status['inside_view'] = True
                    scanning_status['view_header_str_tmp'] = line
                    scanning_status['views_found'] = scanning_status['views_found'] + 1
            # If last line was inside view definition
            else:
                # If last line was inside view definition, then check if current line is also in view definition
                if re.match(f'{VIEW_END_REGEXP}', line, re.IGNORECASE):
                    scanning_status['view_raw_str_tmp'] += line + '\n'
                    scanning_status['inside_view'] = False
                    # Since this is the last line of view definition and we have all view code in variables,
                    # lets make view object
                    tmp_view = view(scanning_status['view_header_str_tmp'], scanning_status['view_raw_str_tmp'],
                                    scanning_status['views_found'])
                    # save create view object for later and continue with setup for next view
                    self.views.append(tmp_view)
                    scanning_status['view_header_str_tmp'] = ''
                    scanning_status['view_raw_str_tmp'] = ''
                # If last line was inside view definition, and current line also is then save code from current line
                else:
                    scanning_status['view_raw_str_tmp'] += line + '\n'

# ---------------------------------------------------------------------------------------------------

    # Simple parser method loading input file
    def load_input_file(self):
        # Try loading the file
        try:
            logging.info(f'Load of input file {self.input_file_path} started')
            with open(f'{self.input_file_path}', 'r') as file:
                self.raw_input_data = file.read()
        # If load fail, then log error and raise it
        except:
            logging.error(f'Cannot load input file {self.input_file_path} !')
            raise ValueError(f'Cannot load input file {self.input_file_path} !')
        logging.info(f'Load of input file {self.input_file_path} finished')

    # Simple parser method for validating input file existence
    def check_input(self):
        logging.info(f'Checking if input file exists')
        # Check if input file exists
        if path.exists(self.input_file_path):
            logging.info(f'Input file \'{self.input_file_path}\' exists')
        # If input file doesn't exist, then delete it
        else:
            logging.error(f'Input file \'{self.input_file_path}\' does not exists !')
            raise ValueError(f'Input file {self.input_file_path} does not exists !')


####################################################################################################
# Main function invoked during script execution                                                    #
####################################################################################################

def start():
    # Set logging level, logging format and log script execution
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logging.info(f'{PROGRAM_NAME} script started')
    # Create new parser
    program_parser = parser()
    # Run this new parser
    program_parser.run()


####################################################################################################
# Input and output parameters for program                                                          #
####################################################################################################

input_parser = argparse.ArgumentParser(prog=f'{PROGRAM_NAME}', formatter_class=argparse.RawTextHelpFormatter,
                                       description=None)
input_parser.add_argument('-i', '--input', help='input directory', required=True)
input_parser.add_argument('-o', '--output', help='output directory', required=True)
args = vars(input_parser.parse_args())

####################################################################################################
# Redirect script start to start function                                                          #
####################################################################################################

if __name__ == '__main__':
    start()
