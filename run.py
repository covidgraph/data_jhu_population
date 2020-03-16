import os
import logging
import py2neo

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('py2neo.connect.bolt').setLevel(logging.WARNING)
logging.getLogger('py2neo.connect').setLevel(logging.WARNING)


from covid_graph import download, load_to_neo4j, helper

ROOT_DIR = '/Users/mpreusse/Downloads/covid'
NEO4J_HOST = 'localhost'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'test'

graph = py2neo.Graph(host=NEO4J_HOST, user=NEO4J_USER, password=NEO4J_PASSWORD)

# setup DB
helper.setup_db(graph)

if not os.path.exists(ROOT_DIR):
    os.makedirs(ROOT_DIR)

# download
jhu_zip_file = download.download_jhu(ROOT_DIR, skip_existing=True)
jhu_dir = download.unzip_file(jhu_zip_file, skip_existing=True)

load_to_neo4j.read_daily_report_JHU(jhu_dir, graph)