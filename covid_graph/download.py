import os
import requests
import zipfile
import logging
import shutil

log = logging.getLogger(__name__)

JHU_GITHUB_ARCHIVE_LINK = 'https://codeload.github.com/CSSEGISandData/COVID-19/zip/master'
JHU_FILE_NAME = 'jhu_covid19.zip'

WPP_AGE_CSV = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_PopulationByAgeSex_Medium.csv'
WPP_FILENAME = 'WPP2019_PopulationByAgeSex_Medium.csv'


def download_jhu(target_dir, skip_existing=False):
    """
    Downlaod the data repository from JHU.

    https://github.com/CSSEGISandData/COVID-19

    :param target_dir: Target directory where to store files.
    :param skip_existing: Do not download if file exists. Default is false, set to true for dev.
    :return: Path to downloaded file.
    """
    log.info('Download JHU data.')
    target_file = os.path.join(target_dir, JHU_FILE_NAME)

    if skip_existing:
        if os.path.exists(target_file):
            log.info("File exists, skip_existing is True, do not download again.")
            return target_file

    log.info('Download to {}'.format(target_file))

    r = requests.get(JHU_GITHUB_ARCHIVE_LINK, allow_redirects=True)

    open(target_file, 'wb').write(r.content)

    return target_file


def download_population_data(target_dir, skip_existing=False):
    """
    Download population data from the UN world population prospect.

    The UN gathers data on world population statistics and publishes the
    world population prospects: https://population.un.org/wpp/

    The latest data set in CSV format can be found here: https://population.un.org/wpp/Download/Standard/CSV/

    :param target_dir: Target directory where to store files.
    :param skip_existing: Do not download if file exists. Default is false, set to true for dev.
    :return: Path to downloaded file.
    """
    log.info('Download UN WPP data')
    target_file = os.path.join(target_dir, WPP_FILENAME)

    if skip_existing:
        if os.path.exists(target_file):
            log.info("File exists, skip_existing is True, do not download again.")
            return target_file

    log.info('Download to {}'.format(target_file))

    r = requests.get(WPP_AGE_CSV, allow_redirects=True)

    open(target_file, 'wb').write(r.content)

    return target_file


def unzip_file(zip_file_path, skip_existing=True):
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
