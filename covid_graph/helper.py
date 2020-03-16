from py2neo.database import ClientError


def setup_db(graph):
    """
    Create Indexes in Neo4j.

    :param graph: The graph instance.
    """

    indexes = [
        ['Country', 'name'],
        ['Province', 'name'],
        ['Update', 'uuid']
    ]

    for index in indexes:
        try:
            graph.schema.create_index(index[0], index[1])
        except ClientError:
            pass
