import logging

log = logging.getLogger(__name__)


def set_latest_update(graph):
    """
    Finde the latest Update for each province and give label :Latest

    :param graph: Neo4j Graph instance
    """
    log.debug("remove all :Latest labels (in case of data update)")
    remove_label_query = "MATCH (n:Latest) REMOVE n:Latest"
    log.debug(remove_label_query)
    graph.run(remove_label_query)

    log.info("Set label :Latest on alst update for each province")

    q = """MATCH (p:Province)-[:REPORTED]->(u:Update)
WITH p, max(u.date) AS latest_date
MATCH (p)-[:REPORTED]->(latest_u:Update)
WHERE latest_u.date = latest_date
SET latest_u:Latest"""

    log.debug(q)

    graph.run(q)
