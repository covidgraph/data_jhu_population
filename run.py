import os
import sys
import logging
import py2neo


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('py2neo.connect.bolt').setLevel(logging.WARNING)
logging.getLogger('py2neo.connect').setLevel(logging.WARNING)
logging.getLogger('graphio').setLevel(logging.WARNING)
logging.getLogger('neobolt').setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# import and setup
from covid_graph import helper, post, jhu, unwpp

ROOT_DIR = os.getenv('ROOT_DIR', '/download')
GC_NEO4J_URL = os.getenv('GC_NEO4J_URL', 'bolt://localhost:7687')
GC_NEO4J_USER = os.getenv('GC_NEO4J_USER', 'neo4j')
GC_NEO4J_PASSWORD = os.getenv('GC_NEO4J_PASSWORD', 'test')
RUN_MODE = os.getenv('RUN_MODE', 'prod')

if RUN_MODE.lower() == 'test':
    import pytest
    log.info("Run tests")
    pytest.main()

else:
    for v in [ROOT_DIR, GC_NEO4J_URL, GC_NEO4J_USER, GC_NEO4J_PASSWORD]:
        log.debug(v)

    graph = py2neo.Graph(GC_NEO4J_URL, user=GC_NEO4J_USER, password=GC_NEO4J_PASSWORD)
    log.debug(graph)

    result = list(graph.run("MATCH (a) RETURN a LIMIT 1"))
    log.debug(result)

    # setup DB
    helper.setup_db(graph)

    if not os.path.exists(ROOT_DIR):
        os.makedirs(ROOT_DIR)
    ###

    # download data
    jhu_zip_file = jhu.download_jhu(ROOT_DIR)
    jhu_dir = helper.unzip_file(jhu_zip_file)

    wpp_csv_file = unwpp.download_population_data(ROOT_DIR, skip_existing=True)
    ###

    # load to Neo4j
    jhu.read_daily_report_JHU(jhu_dir, graph)
    unwpp.load_wpp_data(ROOT_DIR, graph)
    ###

    # post process
    post.set_latest_update(graph)
    ###
