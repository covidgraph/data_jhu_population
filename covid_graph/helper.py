import os
import shutil
import zipfile

from py2neo.database import ClientError


def setup_db(graph):
    """
    Create Indexes in Neo4j.

    :param graph: The graph instance.
    """

    indexes = [
        ['Country', 'name'],
        ['Province', 'name'],
        ['DailyReport', 'uuid'],
        ['AgeGroup', 'group']
    ]

    for index in indexes:
        try:
            graph.schema.create_index(index[0], index[1])
        except ClientError:
            pass


def unzip_file(zip_file_path, skip_existing=False):
    """
    Unzip a zip file at the same directory. Return the path to the unzipped directory.

    Note: By default the data is not overwritten. Remove target directory before unzipping.

    :param zip_file_path: Path to the zip file.
    :param skip_existing: Do not unzip if directory exists. Default is false, set to true for dev.
    :return: Path to unzipped directory.
    """
    zip_file_directory = os.path.dirname(zip_file_path)
    zip_file_name = os.path.basename(zip_file_path)

    target_directory = os.path.join(zip_file_directory, zip_file_name.replace('.zip', ''))

    if skip_existing:
        if os.path.exists(target_directory):
            log.info("Unzipped directory exists, skip_existing is True, do not download again.")
            return target_directory

    if os.path.exists(target_directory):
        log.debug("Target directory exists already {}".format(target_directory))
        log.debug("Delete to unzip again.")
        shutil.rmtree(target_directory)

    log.debug('Unzip {} to {}'.format(zip_file_path, target_directory))

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(target_directory)

    return target_directory
