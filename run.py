import os
import logging
import py2neo
import json

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('py2neo.client.bolt').setLevel(logging.WARNING)
logging.getLogger('py2neo.client').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('graphio').setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# import and setup
from covid_graph import helper, post, jhu, unwpp

ROOT_DIR = os.getenv('ROOT_DIR', '/download')
RUN_MODE = os.getenv('RUN_MODE', 'prod')

NEO4J_CONFIG_STRING = os.getenv("NEO4J")

try:
    log.info(NEO4J_CONFIG_STRING)
    NEO4J_CONFIG_DICT = json.loads(NEO4J_CONFIG_STRING)
except json.decoder.JSONDecodeError:
    # try to replace single quotes with double quotes
    # JSON always expects double quotes, common mistake when writing JSON strings
    NEO4J_CONFIG_STRING = NEO4J_CONFIG_STRING.replace("'", '"')
    log.info(NEO4J_CONFIG_STRING)
    NEO4J_CONFIG_DICT = json.loads(NEO4J_CONFIG_STRING)

if RUN_MODE.lower() == 'test':
    import pytest

    log.info("Run tests")
    pytest.main()

else:

    graph = py2neo.Graph(host=NEO4J_CONFIG_DICT['host'], user=NEO4J_CONFIG_DICT['user'],
                         password=NEO4J_CONFIG_DICT['password'], secure=NEO4J_CONFIG_DICT['secure'], verify=False)

    # setup DB
    helper.setup_db(graph)

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
