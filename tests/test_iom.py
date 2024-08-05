import logging
from os.path import join

import pytest
from freezegun import freeze_time

from hdx.api.configuration import Configuration
from hdx.scraper.iom.iom import IOM
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def expected_json() -> dict:
    return {
        "web_id": "2014.MMP01037",
        "region": "North America",
        "reported_date": "2014-12-31",
        "number_dead": "1",
        "number_missing": "",
        "total_dead_and_missing": "1",
        "number_of_survivors": "",
        "number_of_female": "",
        "number_of_male": "1",
        "number_of_children": "",
        "cause_death": "Unknown (skeletal remains)",
        "country_of_incident": "United States of America",
        "location_description": "Pima Country Office of the Medical Examiner jurisdiction, Arizona, USA (see coordinates for exact location)\n",
        "unsd_geographic_grouping": "Northern America",
        "location_coodinates": "32.22804, -112.590416",
        "migration_route": "US-Mexico border crossing",
        "information_source": "Pima County Office of the Medical Examiner (PCOME)",
        "url": "http://humaneborders.info/",
        "source_quality": "5",
    }


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "caveats": "Total figures include 331 deaths that are not included in the monthly breakdown because the month in which the deaths occured is not specified; 88 of these occured on the U.S. Mexico border and 243 in the Bay of Bengal (Southeast Asia). In the above chart, totals by region are correct, but monthly totals are incomplete for some months. Note: Europe refers to the region generally, and not the European Union.",
        "data_update_frequency": 7,
        "dataset_date": "[2014-01-02T00:00:00 TO 2024-07-28T23:59:59]",
        "dataset_source": "IOM",
        "groups": [{"name": "world"}],
        "license_id": "cc-by-igo",
        "maintainer": "b682f6f7-cd7e-4bd4-8aa7-f74138dc6313",
        "methodology": "http://missingmigrants.iom.int/methodology",
        "name": "missing-migrants-project-data",
        "notes": "Missing Migrants Project draws on a range of sources to track deaths of migrants along migratory routes across the globe. Data from this project are published in the report “Fatal Journeys: Tracking Lives Lost during Migration,” which provides the most comprehensive global tally of migrant fatalities since 2014. What is included in Missing Migrants Project data? Missing Migrants Project counts migrants who have died at the external borders of states, or in the process of migration towards an international destination, regardless of their legal status. The Project records only those migrants who die during their journey to a country different from their country of residence. Missing Migrants Project data include the deaths of migrants who die in transportation accidents, shipwrecks, violent attacks, or due to medical complications during their journeys. It also includes the number of corpses found at border crossings that are categorized as the bodies of migrants, on the basis of belongings and/or the characteristics of the death. For instance, a death of an unidentified person might be included if the decedent is found without any identifying documentation in an area known to be on a migration route. Deaths during migration may also be identified based on the cause of death, especially if is related to trafficking, smuggling, or means of travel such as on top of a train, in the back of a cargo truck, as a stowaway on a plane, in unseaworthy boats, or crossing a border fence. While the location and cause of death can provide strong evidence that an unidentified decedent should be included in Missing Migrants Project data, this should always be evaluated in conjunction with migration history and trends. What is excluded? The count excludes deaths that occur in immigration detention facilities or after deportation to a migrant’s homeland, as well as deaths more loosely connected with migrants´ irregular status, such as those resulting from labour exploitation. Migrants who die or go missing after they are established in a new home are also not included in the data, so deaths in refugee camps or housing are excluded. The deaths of internally displaced persons who die within their country of origin are also excluded. There remains a significant gap in knowledge and data on such deaths. Data and knowledge of the risks and vulnerabilities faced by migrants in destination countries, including death, should not be neglected, but rather tracked as a distinct category.\n",
        "owner_org": "50dcc50c-84ee-4350-98fe-a9493b52742f",
        "package_creator": "HDX Data Systems Team",
        "private": False,
        "subnational": False,
        "tags": [
            {
                "name": "asylum seekers",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "migration",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "refugees",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "title": "Missing Migrants Project Data",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "description": "CSV file containing numbers of migrants who have died or gone missing in the process of migration towards an international destination since 2014.",
            "format": "csv",
            "name": "iom-missing-migrants-project-data.csv",
            "resource_type": "file.upload",
            "url_type": "upload",
        }
    ]


@pytest.fixture
def mock_get_mapped_tags(mocker):
    return mocker.patch(
        "hdx.data.vocabulary.Vocabulary.get_mapped_tags",
        return_value=(["asylum seekers", "migration", "refugees"], []),
    )


class TestIOM:
    @pytest.fixture(scope="class")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="dev",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "iom", "config")

    @freeze_time("2024-07-30")
    def test_iom(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        expected_json,
        expected_dataset,
        expected_resources,
        mock_get_mapped_tags,
    ):
        with temp_dir(
            "TestIOM",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )

                iom = IOM(
                    configuration=configuration,
                    retriever=retriever,
                    temp_dir=tempdir,
                )

                data_by_year_list = iom.scrape_data()

                assert list(data_by_year_list[0].keys()) == [
                    "web_id",
                    "region",
                    "reported_date",
                    "number_dead",
                    "number_missing",
                    "total_dead_and_missing",
                    "number_of_survivors",
                    "number_of_female",
                    "number_of_male",
                    "number_of_children",
                    "cause_death",
                    "country_of_incident",
                    "location_description",
                    "unsd_geographic_grouping",
                    "location_coodinates",
                    "migration_route",
                    "information_source",
                    "url",
                    "source_quality",
                ]
                dataset = iom.generate_dataset(
                    data_by_year_list=data_by_year_list
                )
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == expected_dataset
                resources = dataset.get_resources()
                assert resources == expected_resources

                filename_list = [
                    "iom-missing-migrants-project-data.csv",
                ]
                for filename in filename_list:
                    assert_files_same(
                        join("tests", "fixtures", filename),
                        join(tempdir, filename),
                    )
