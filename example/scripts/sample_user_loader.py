"""
This is a example script which demo how to load data into neo4j without using Airflow DAG.
"""

import csv
import logging
from pyhocon import ConfigFactory
import sqlite3
from sqlalchemy.ext.declarative import declarative_base
import textwrap

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

DB_FILE = '/tmp/test.db'
SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
Base = declarative_base()

# replace localhost with docker host ip
# todo: get the ip from input argument
NEO4J_ENDPOINT = 'bolt://localhost:7687'
neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception:
        logging.exception('exception')
    return None


def load_table_data_from_csv(file_name):
    conn = create_connection(DB_FILE)
    if conn:
        cur = conn.cursor()
        cur.execute('drop table if exists user_metadata')
        cur.execute('create table if not exists user_metadata ('
                    'email VARCHAR(64) NOT NULL, '
                    'first_name VARCHAR(64) NOT NULL, '
                    'last_name VARCHAR(64) NOT NULL, '
                    'name VARCHAR(64) NOT NULL, '
                    'github_username VARCHAR(64) NOT NULL, '
                    'team_name VARCHAR(64) NOT NULL, '
                    'employee_type VARCHAR(64) NOT NULL, '
                    'manager_email VARCHAR(64) NOT NULL, '
                    'slack_id VARCHAR(64) NOT NULL, '
                    'is_active INT NOT NULL, '
                    'updated_at INT NOT NULL ) ')
        file_loc = 'example/sample_data/' + file_name
        with open(file_loc, 'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['email'],
                      i['first_name'],
                      i['last_name'],
                      i['name'],
                      i['github_username'],
                      i['team_name'],
                      i['employee_type'],
                      i['manager_email'],
                      i['slack_id'],
                      i['is_active'],
                      i['updated_at']) for i in dr]

        cur.executemany("INSERT INTO user_metadata ("
                        "email, "
                        "last_name, "
                        "first_name, "
                        "name, "
                        "github_username, "
                        "employee_type, "
                        "team_name, "
                        "manager_email, "
                        "slack_id, "
                        "is_active, "
                        "updated_at ) VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
        conn.commit()


def create_sample_job(table_name, model_name):
    sql = textwrap.dedent("""
    select * from {table_name};
    """).format(table_name=table_name)

    tmp_folder = '/var/tmp/amundsen/{table_name}'.format(table_name=table_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    sql_extractor = SQLAlchemyExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): SQLITE_CONN_STRING,
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.EXTRACT_SQL): sql,
        'extractor.sqlalchemy.model_class': model_name,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


if __name__ == "__main__":
    load_table_data_from_csv('sample_user.csv')
    if create_connection(DB_FILE):
        # start table job
        job1 = create_sample_job('user_metadata',
                                 'databuilder.models.user.User')
        job1.launch()
