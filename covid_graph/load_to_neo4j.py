import os
import logging
import csv
from uuid import uuid4
from graphio import NodeSet, RelationshipSet
from dateutil.parser import parse

log = logging.getLogger(__name__)


def read_daily_report_JHU(path_to_jhu, graph):
    """
    Data from JHU contains aggregated daily reports on Covid-19 cases by country/region.

    :param path_to_jhu: The path to the unzipped JHU data repository.
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
            updates.create(graph)

            province_rep_update.create(graph)
            province_in_country.create(graph)


def read_daily_report_data_csv_JHU(file):
    """
    Extract data from a single daile report file from JHU.

    :param file: Path to the CSV file
    :return:
    """
    log.info('Read JHU CSV file {}'.format(file))

    countries = NodeSet(['Country'], ['name'])
    provinces = NodeSet(['Province'], ['name'])
    updates = NodeSet(['Update'], ['uuid'])
    province_in_country = RelationshipSet('PART_OF', ['Province'], ['Country'], ['name'], ['name'])
    province_in_country.unique = True
    province_rep_update = RelationshipSet('REPORTED', ['Province'], ['Update'], ['name'], ['uuid'])

    with open(file, 'rt') as csvfile:
        rows = csv.reader(csvfile, delimiter=',', quotechar='"')
        # skip header
        next(rows)

        for row in rows:
            uuid = str(uuid4())
            province = row[0]
            country = row[1]
            date = parse(row[2])
            confirmed = int(row[3]) if row[3] else 'na'
            death = int(row[4]) if row[4] else 'na'
            recovered = int(row[5]) if row[5] else 'na'

            lat = row[6] if len(row) >= 7 else None
            long = row[7] if len(row) >= 8 else None

            province_dict = {'name': province}
            if lat and long:
                province_dict['latitude'] = lat
                province_dict['longitude'] = long
            provinces.add_unique(province_dict)

            countries.add_unique({'name': country})

            updates.add_unique(
                {'date': date, 'confirmed': confirmed, 'death': death, 'recovered': recovered, 'uuid': uuid})

            province_in_country.add_relationship({'name': province}, {'name': country}, {'source': 'jhu'})
            province_rep_update.add_relationship({'name': province}, {'uuid': uuid}, {'source': 'jhu'})

    return countries, provinces, updates, province_in_country, province_rep_update
