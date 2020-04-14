import csv
import os
import logging
import requests
from graphio import NodeSet, RelationshipSet


log = logging.getLogger(__name__)

WPP_AGE_CSV = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_PopulationByAgeSex_Medium.csv'
WPP_FILENAME = 'WPP2019_PopulationByAgeSex_Medium.csv'


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