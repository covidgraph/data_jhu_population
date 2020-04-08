import os
import logging
import csv
from uuid import uuid4
from graphio import NodeSet, RelationshipSet
from dateutil.parser import parse
from dateutil.parser import ParserError

log = logging.getLogger(__name__)


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

            log.debug('MERGE countries')
            countries.merge(graph)
            log.debug('MERGE provinces')
            provinces.merge(graph)
            log.debug('CREATE Updates')
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
        log.debug("Cannot parse date string {}".format(row[2]))

    try:
        confirmed = int(row[3])
    except ValueError:
        log.debug("Cannot parse integer {}".format(row[3]))
        confirmed = 'na'

    try:
        death = int(row[4])
    except ValueError:
        log.debug("Cannot parse integer {}".format(row[4]))
        death = 'na'

    try:
        recovered = int(row[5])
    except ValueError:
        log.debug("Cannot parse integer {}".format(row[5]))
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
        log.debug("Cannot parse integer {}".format(row[7]))
        confirmed = 'na'

    try:
        death = int(row[8])
    except ValueError:
        log.debug("Cannot parse integer {}".format(row[8]))
        death = 'na'

    try:
        recovered = int(row[9])
    except ValueError:
        log.debug("Cannot parse integer {}".format(row[9]))
        recovered = 'na'

    lat = row[5]
    long = row[6]

    return country, province, date, confirmed, death, recovered, lat, long


def load_wpp_data(base_path, graph):
    """
    Load UN population data.

    :param base_path: Path where file was downloaded.
    """
    un_wpp_csv_file = os.path.join(base_path, 'WPP2019_PopulationByAgeSex_Medium.csv')
    log.info('Parse UN population data file: {}'.format(un_wpp_csv_file))

    country = NodeSet(['Country'], ['name'])
    age_group_nodes = NodeSet(['AgeGroup'], ['group'])
    country_total_group = RelationshipSet('CURRENT_TOTAL', ['Country'], ['AgeGroup'], ['name'], ['group'])
    country_male_group = RelationshipSet('CURRENT_MALE', ['Country'], ['AgeGroup'], ['name'], ['group'])
    country_female_group = RelationshipSet('CURRENT_FEMALE', ['Country'], ['AgeGroup'], ['name'], ['group'])

    countries_added = set()
    age_groups_added = set()

    with open(un_wpp_csv_file, 'rt') as f:
        csv_file = csv.reader(f, delimiter=',', quotechar='"')
        # skip header
        next(csv_file)
        for row in csv_file:
            # LocID,Location,VarID,Variant,Time,MidPeriod,AgeGrp,AgeGrpStart,AgeGrpSpan,PopMale,PopFemale,PopTotal
            loc_id = row[0]
            location = row[1]
            time = int(row[4])
            age_group = row[6]
            age_group_start = int(row[7])
            age_group_span = row[8]
            pop_male = int(float((row[9])) * 1000)
            pop_female = int(float((row[10])) * 1000)
            pop_total = int(float((row[11])) * 1000)

            # only take 2019
            if time == 2019:
                if location not in countries_added:
                    country.add_node({'name': location, 'un_id': loc_id})
                    countries_added.add(location)
                if age_group not in age_groups_added:
                    age_group_nodes.add_node({'group': age_group, 'start': age_group_start, 'span': age_group_span})

                country_total_group.add_relationship({'name': location}, {'group': age_group}, {'count': pop_total})
                country_male_group.add_relationship({'name': location}, {'group': age_group}, {'count': pop_male})
                country_female_group.add_relationship({'name': location}, {'group': age_group}, {'count': pop_female})

    log.info('Load data to Neo4j')
    country.merge(graph)
    age_group_nodes.merge(graph)
    country_total_group.merge(graph)
    country_male_group.merge(graph)
    country_female_group.merge(graph)
