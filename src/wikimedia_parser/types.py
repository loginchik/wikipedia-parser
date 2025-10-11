import datetime as dt
import re
from typing import NamedTuple, Dict, Any, Tuple

import numpy as np
import pandas as pd

from .enums import DateGranularity, AccessType, UserAgent


class PageStatisticsRecord(NamedTuple):
    """
    One row of a returned data
    """

    project: str
    article: str
    granularity: DateGranularity
    timestamp: dt.date
    access: AccessType
    agent: UserAgent
    views: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PageStatisticsRecord":
        """
        Converts a dict representation from the response into the item of the class

        :param data: dictionary from the response
        :return: item of a class
        """
        updated_data = data.copy()
        updated_data["timestamp"] = dt.datetime.strptime(data["timestamp"], "%Y%m%d00").date()
        updated_data["access"] = AccessType(data["access"])
        updated_data["granularity"] = DateGranularity(data["granularity"])
        updated_data["agent"] = UserAgent(data["agent"])
        updated_data["views"] = int(data["views"])
        return cls(**updated_data)

    def __hash__(self):
        return hash((self.project, self.article, self.granularity, self.timestamp, self.access, self.agent))

    def __eq__(self, other: "PageStatisticsRecord") -> bool:
        if not isinstance(other, PageStatisticsRecord):
            return NotImplemented
        return all(
            [
                getattr(self, attr) == getattr(other, attr)
                for attr in ["project", "article", "granularity", "timestamp", "access", "agent"]
            ]
        )


class PageStatistics:
    """
    One article statistics
    """

    def __init__(self, *records: PageStatisticsRecord) -> None:
        self.article = records[0].article
        self.granularity = records[0].granularity
        self.access = records[0].access
        self.agent = records[0].agent
        self.project = records[0].project

        if any(
            [
                record.article != self.article
                or record.granularity != self.granularity
                or record.access != self.access
                or record.agent != self.agent
                or record.project != self.project
                for record in records
            ]
        ):
            raise ValueError("Inconsistent records")
        self.records = sorted(list(set(records)), key=lambda r: r.timestamp)

    def to_df(self) -> pd.DataFrame:
        """
        Converts the records array into a pandas DataFrame

        :return: pandas DataFrame
        """
        df = pd.DataFrame([record._asdict() for record in self.records])
        df["granularity"] = pd.Categorical(df["granularity"])
        df["access"] = pd.Categorical(df["access"])
        df["agent"] = pd.Categorical(df["agent"])
        df["project"] = pd.Categorical(df["project"])
        df["article"] = pd.Categorical(df["article"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d")
        return df

    @property
    def records_count(self) -> int:
        """
        :return: number of records stored
        """
        return len(self.records)

    @property
    def start_date(self) -> dt.date:
        """
        :return: earliest date of the records
        """
        return sorted(self.records, key=lambda r: r.timestamp)[0].timestamp

    @property
    def end_date(self) -> dt.date:
        """
        :return: latest date of the records
        """
        return sorted(self.records, key=lambda r: r.timestamp)[-1].timestamp

    @property
    def top_views_record(self) -> PageStatisticsRecord:
        """
        :return: the record with the most number of views
        """
        return sorted(self.records, key=lambda r: r.views)[-1]

    @property
    def total_views(self) -> int:
        """
        :return: total number of views stored in the article
        """
        return int(np.sum([record.views for record in self.records]))

    @property
    def url(self) -> str:
        """
        :return: dynamically collected article URL
        """
        return f"https://{self.project}.org/wiki/{self.article}"


class WikimediaRequest(NamedTuple):
    """
    Wikimedia page request
    """

    url: str  #: Target page full URL, example: https://en.wikipedia/wiki/example
    start_timestamp: dt.date
    end_timestamp: dt.date
    granularity: DateGranularity = DateGranularity.Daily
    access: AccessType = AccessType.Any
    agent: UserAgent = UserAgent.User

    @property
    def as_url(self) -> str:
        """
        Converts the request into a URL path
        :return: prepared URL path to pass into parser's client
        """
        project, article = self._parse_url()
        start_timestamp, end_timestamp = list(
            map(
                lambda x: x.strftime("%Y%m%d00"),
                sorted([self.start_timestamp, self.end_timestamp]),
            )
        )
        return f"/{project}/{self.access.value}/{self.agent.value}/{article}/{self.granularity.value}/{start_timestamp}/{end_timestamp}"

    def _parse_url(self) -> Tuple[str, str]:
        """
        Extracts project name (equivalent to language) and article name
        from the full URL to Wiki page

        :return: project, article
        :raise ValueError: if unable to process regular expression
        """
        try:
            project, article = re.match(r"https://(\w+\.wikipedia).org/wiki/([^/#]+)", self.url).groups()
            return project, article
        except AttributeError as e:
            raise ValueError(f"Unprocessable URL: {self.url}. Unable to extract project and article title") from e
