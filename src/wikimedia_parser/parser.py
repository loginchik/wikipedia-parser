import asyncio
import datetime as dt
from logging import getLogger, Logger
from typing import List

import httpx
import pandas as pd

from .enums import DateGranularity, AccessType, UserAgent
from .types import (
    WikimediaRequest,
    PageStatistics,
    PageStatisticsRecord,
)


class WikimediaParser:
    """
    Wikimedia parser to collect page visits count

    Provides methods to collect statistics from Wikimedia pages. Use ``get_page_statistics``
    to gather information on 1 article, or ``get_multiple_pages_statistics`` to request multiple pages
    at the same time. The collected statistics can be concatenated into one pandas DataFrame
    via ``concat_statistics`` method
    """

    def __init__(self, timeout: int = 60, max_connections: int = 10) -> None:
        self._client = None
        self._logger = None
        self._timeout = timeout
        self._max_connections = max_connections

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url="https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article",
                timeout=httpx.Timeout(self._timeout),
                follow_redirects=True,
                limits=httpx.Limits(max_connections=self._max_connections),
            )
        return self._client

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = getLogger("wikimedia.parser")
        return self._logger

    async def get_page_statistics(self, request: WikimediaRequest) -> PageStatistics:
        """
        Queries one-page statistics

        :param request: WikimediaRequest object with specified params
        :return: page statistics with all records
        """
        response = await self.client.get(url=request.as_url)
        if response.status_code != 200:
            raise ConnectionError(f"Wikimedia page request failed with status code {response.status_code}")
        self.logger.debug(f"Wikimedia page request succeeded: {request.url}")
        return PageStatistics(*[PageStatisticsRecord.from_dict(elem) for elem in response.json()["items"]])

    async def get_multiple_pages_statistics(
        self,
        start_date: dt.date,
        end_date: dt.date,
        pages: List[str],
        granularity: DateGranularity = DateGranularity.Daily,
        access: AccessType = AccessType.Any,
        agent: UserAgent = UserAgent.User,
        chunk_size: int = 10,
    ) -> List[PageStatistics]:
        """
        Gathers multiple pages' statistics in chunks

        Splits the provided pages into an array of chunks, that are loaded simultaneously, and gathers the data.
        If any loading process raises error, cancels all other tasks and throws the exception

        :param start_date: start date
        :param end_date: end date
        :param pages: URLs to collect
        :param granularity:
        :param access:
        :param agent:
        :param chunk_size: number of pages to load at the same time
        :return: list of pages' statistics
        """
        collected = []
        pages = list(set(pages))
        for chunk in (pages[i : i + chunk_size] for i in range(0, len(pages), chunk_size)):
            requests = [
                WikimediaRequest(
                    url=page,
                    start_timestamp=start_date,
                    end_timestamp=end_date,
                    granularity=granularity,
                    access=access,
                    agent=agent,
                )
                for page in chunk
            ]
            tasks = [asyncio.create_task(self.get_page_statistics(req)) for req in requests]
            try:
                chunk_results = await asyncio.gather(*tasks)
                collected.extend(chunk_results)
            except Exception as e:
                self.logger.warning("Error occurred while collecting pages. Cancelling remaining tasks...")
                for task in tasks:
                    if not task.done():
                        task.cancel()
                self.logger.debug("Remaining tasks cancelled.")
                raise e
        return collected

    @staticmethod
    def concat_statistics(*statistics: PageStatistics) -> pd.DataFrame:
        """
        Converts statistics into pandas DataFrames and concatenates them.
        Preserves uniqueness of the collected data

        :param statistics: statistics
        :return: pandas DataFrame
        """
        return (
            pd.concat([st.to_df() for st in statistics], ignore_index=True, axis=0)
            .drop_duplicates()
            .reset_index(drop=True)
        )
