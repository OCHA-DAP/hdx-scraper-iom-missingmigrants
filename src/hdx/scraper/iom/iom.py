import logging
from datetime import datetime
from typing import List, Optional

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class IOM:
    _DATE_FIELD = "reported_date"
    _FILENAME = "iom-missing-migrants-project-data.csv"
    _LOCATION = "world"
    _OUTPUT_FORMAT = "json"
    _HXLTAGS = {
        "web_id": "#web+id",
        "region": "#region+name",
        "reported_date": "#date+reported",
        "number_dead": "#affected+killed",
        "number_missing": "#affected+missing",
        "total_dead_and_missing": "#affected+killed+missing+total",
        "number_of_survivors": "#affected+survivors",
        "number_of_female": "#affected+f",
        "number_of_male": "#affected+m",
        "number_of_children": "#affected+children",
        "cause_death": "#cause+type",
        "country_of_incident": "#country+event",
        "location_description": "#loc+description",
        "unsd_geographic_grouping": "#region+unsd+name",
        "location_coodinates": "#geo+coord",
        "migration_route": "#route+description",
        "information_source": "#meta+source+name",
        "url": "#meta+url",
        "source_quality": "#meta+source+quality",
    }

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._tags = self._create_tags()

    def scrape_data(self) -> list:
        """
        Query the API by year, and store the results in a list.

        The result for a single row will have the following format:
        {
            "web_id": "2014.MMP01037",
            "region": "North America",
            "reported_date": "2014-12-31",
            etc.
        }
        """
        logger.info("Scraping data")
        data_url = self._configuration["base_url"]

        start_year = 2014
        next_year = datetime.now().year + 1
        years = [year for year in range(start_year, next_year)]

        data_by_year_list = []
        for year in years:
            data_url_year = f"{data_url}/{year}/{self._OUTPUT_FORMAT}"
            data = self._retriever.download_json(data_url_year)
            if not data:
                logger.info(f"No data for {year}")
                continue
            logger.info(f"Found {len(data)} rows for {year}")
            data_by_year_list.extend(data)
        return data_by_year_list

    def generate_dataset(self, data_by_year_list: List) -> Optional[Dataset]:
        """
        Generate the dataset
        """
        # Setup the dataset information
        title = "Missing Migrants Project Data"
        slugified_name = slugify("Missing Migrants Project Data")

        logger.info(f"Creating dataset: {title}")

        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )

        date_range = self._get_date_range(data_by_year_list)

        dataset.set_time_period(
            startdate=date_range["min_date"], enddate=date_range["max_date"]
        )
        dataset.add_other_location(self._LOCATION)
        dataset.add_tags(self._tags)

        resource_data = {
            "name": self._FILENAME,
            "description": "CSV file containing numbers of migrants who have died or gone missing in the process of migration towards an international destination since 2014.",
        }

        dataset.generate_resource_from_iterable(
            list(data_by_year_list[0].keys()),
            data_by_year_list,
            self._HXLTAGS,
            self._temp_dir,
            self._FILENAME,
            resource_data,
            self._DATE_FIELD,
            quickcharts=None,
        )

        return dataset

    def _create_tags(self) -> List[str]:
        logger.info("Generating tags")
        tags = self._configuration["fixed_tags"]
        return tags

    def _get_date_range(self, data_by_year_list: List) -> dict:
        """
        Get min and max dates from dataset
        """
        # Convert reported_date to datetime object
        dates = []
        for row in data_by_year_list:
            date_str = row.get(self._DATE_FIELD)
            if date_str:
                try:
                    dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
                except ValueError as e:
                    print(f"Error parsing date '{date_str}': {e}")

        min_date = min(dates)
        max_date = max(dates)

        return {"min_date": min_date, "max_date": max_date}
