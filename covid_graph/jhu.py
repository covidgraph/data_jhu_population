import csv
import os
import logging

import requests
from dateutil.parser import parse, ParserError
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)

JHU_GITHUB_ARCHIVE_LINK = 'https://codeload.github.com/CSSEGISandData/COVID-19/zip/master'
JHU_FILE_NAME = 'jhu_covid19.zip'


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


def read_daily_report_JHU(path_to_jhu, graph):
    """
    Data from JHU contains aggregated daily reports on Covid-19 cases by country/region.

    :param path_to_jhu: The path to the directory where the zip file was extracted (name of extracted directory is not the same name as zip file.
    :param graph: Neo4j Graph instance.
    """
    log.info('Read daily reports from JHU.')

    daily_report_directory = os.path.join(path_to_jhu, 'COVID-19-master/csse_covid_19_data/csse_covid_19_daily_reports')

    log.debug('Daily reports directory: {}'.format(daily_report_directory))

    for file in os.listdir(daily_report_directory):
        if '.csv' in file:
            file_path = os.path.join(daily_report_directory, file)
            countries, provinces, updates, province_in_country, province_rep_update = read_daily_report_data_csv_JHU(
                file_path)

            countries.merge(graph)
            provinces.merge(graph)
            updates.merge(graph)

            province_rep_update.merge(graph)
            province_in_country.merge(graph)


def read_daily_report_data_csv_JHU(file):
    """
    Extract data from a single daile report file from JHU.

    Old format (until 03-21-2020)
        Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude
    New format:
        FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key

    :param file: Path to the CSV file
    :return:
    """
    log.info('Read JHU CSV file {}'.format(file))
    # understand if old fromat (

    countries = NodeSet(['Country'], ['name'])
    provinces = NodeSet(['Province'], ['name'])
    updates = NodeSet(['DailyReport'], ['uuid'])
    province_in_country = RelationshipSet('PART_OF', ['Province'], ['Country'], ['name'], ['name'])
    province_in_country.unique = True
    province_rep_update = RelationshipSet('REPORTED', ['Province'], ['DailyReport'], ['name'], ['uuid'])

    with open(file, 'rt') as csvfile:
        rows = csv.reader(csvfile, delimiter=',', quotechar='"')
        # skip header
        header = next(rows)
        if len(header) > 8:
            file_type = 'new'
        else:
            file_type = 'old'
        log.info("File type: {}".format(file_type))

        for row in rows:

            if file_type == 'old':
                country, province, date, confirmed, death, recovered, lat, long = parse_jhu_old_file_row(row)
            elif file_type == 'new':
                country, province, date, confirmed, death, recovered, lat, long = parse_jhu_new_file_row(row)

            province_dict = {'name': province}
            if lat and long:
                province_dict['latitude'] = lat
                province_dict['longitude'] = long

            uuid = country + province + str(date)

            provinces.add_unique(province_dict)

            countries.add_unique({'name': country})

            updates.add_unique(
                {'date': date, 'confirmed': confirmed, 'death': death, 'recovered': recovered, 'uuid': uuid})

            province_in_country.add_relationship({'name': province}, {'name': country}, {'source': 'jhu'})
            province_rep_update.add_relationship({'name': province}, {'uuid': uuid}, {'source': 'jhu'})

    return countries, provinces, updates, province_in_country, province_rep_update


def parse_jhu_old_file_row(row):
    """
    Old format (until 03-21-2020)
        Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude

    :param row:
    """
    country = row[1]
    province = row[0]
    # if no name for province, use country name
    if not province:
        province = '{}_complete'.format(country)

    date = None
    try:
        date = parse(row[2])
    except ParserError:
        pass
        #log.debug("Cannot parse date string {}".format(row[2]))

    try:
        confirmed = int(row[3])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[3]))
        confirmed = 'na'

    try:
        death = int(row[4])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[4]))
        death = 'na'

    try:
        recovered = int(row[5])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[5]))
        recovered = 'na'

    lat = row[6] if len(row) >= 7 else None
    long = row[7] if len(row) >= 8 else None

    return country, province, date, confirmed, death, recovered, lat, long


def parse_jhu_new_file_row(row):
    """
    New format:
        FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key

    :param row:
    :return:
    """
    country = row[3]
    province = row[2]
    # if no name for province, use country name
    if not province:
        province = '{}_complete'.format(country)

    date = None
    try:
        date = parse(row[4])
    except ParserError:
        log.debug("Cannot parse date string {}".format(row[2]))

    try:
        confirmed = int(row[7])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[7]))
        confirmed = 'na'

    try:
        death = int(row[8])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[8]))
        death = 'na'

    try:
        recovered = int(row[9])
    except ValueError:
        #log.debug("Cannot parse integer {}".format(row[9]))
        recovered = 'na'

    lat = row[5]
    long = row[6]

    return country, province, date, confirmed, death, recovered, lat, long