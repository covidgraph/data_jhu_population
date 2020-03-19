import os
import logging
import py2neo

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('py2neo.connect.bolt').setLevel(logging.WARNING)
logging.getLogger('py2neo.connect').setLevel(logging.WARNING)

# import and setup
from covid_graph import download, load_to_neo4j, helper, post

ROOT_DIR = os.getenv('ROOT_DIR')
NEO4J_URL = os.getenv('NEO4J_URL')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

graph = py2neo.Graph(NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD)


# setup DB
helper.setup_db(graph)

if not os.path.exists(ROOT_DIR):
    os.makedirs(ROOT_DIR)
###

# download data
jhu_zip_file = download.download_jhu(ROOT_DIR)
jhu_dir = download.unzip_file(jhu_zip_file)

wpp_csv_file = download.download_population_data(ROOT_DIR, skip_existing=True)
###

# load to Neo4j
load_to_neo4j.read_daily_report_JHU(jhu_dir, graph)
load_to_neo4j.load_wpp_data(ROOT_DIR, graph)
###

# post process
post.set_latest_update(graph)
###