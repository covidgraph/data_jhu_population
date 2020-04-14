import pytest
import requests

JHU_GITHUB_ARCHIVE_LINK = 'https://codeload.github.com/CSSEGISandData/COVID-19/zip/master'
JHU_FILE_NAME = 'jhu_covid19.zip'
WPP_AGE_CSV = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_PopulationByAgeSex_Medium.csv'

@pytest.mark.runtest
def test_jhu_available():
    r = requests.head(JHU_GITHUB_ARCHIVE_LINK, allow_redirects=True)

    assert r.status_code == 200


def test_wpp_available():
    r = requests.head(WPP_AGE_CSV, allow_redirects=True)

    assert r.status_code == 200
