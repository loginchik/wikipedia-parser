import datetime as dt

import pandas as pd
import pytest

from src.wikimedia_parser.types import PageStatisticsRecord, PageStatistics, WikimediaRequest
from src.wikimedia_parser.enums import DateGranularity, AccessType, UserAgent


@pytest.mark.parametrize('access', [a.value for a in AccessType])
@pytest.mark.parametrize('granularity', [g.value for g in DateGranularity])
@pytest.mark.parametrize('user_agent', [ua.value for ua in UserAgent])
def test_record_from_dict(access: str, granularity: str, user_agent: str) -> None:
    result = PageStatisticsRecord.from_dict({
        'timestamp': '2025010100',
        'access': access,
        'granularity': granularity,
        'agent': user_agent,
        'views': '100',
        'project': 'ru.wikipedia',
        'article': 'test-article'
    })

    assert isinstance(result, PageStatisticsRecord)
    assert isinstance(result.access, AccessType)
    assert isinstance(result.granularity, DateGranularity)
    assert isinstance(result.agent, UserAgent)
    assert isinstance(result.views, int)
    assert result.project == 'ru.wikipedia'
    assert result.article == 'test-article'


@pytest.mark.parametrize(('this', 'other', 'result'), [
    (
            PageStatisticsRecord(
                project='project', article='article', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=10
            ),
            PageStatisticsRecord(
                project='project', article='article', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=15
            ),
            True
    ),
    (
            PageStatisticsRecord(
                project='project-1', article='article', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=10
            ),
            PageStatisticsRecord(
                project='project', article='article', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=10
            ),
            False
    ),
    (
            PageStatisticsRecord(
                project='project', article='article-1', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=10
            ),
            PageStatisticsRecord(
                project='project', article='article', granularity=DateGranularity.Daily,
                timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
                agent=UserAgent.Any, views=10
            ),
            False
    )
])
def test_records_equal(this: PageStatisticsRecord, other: PageStatisticsRecord, result: bool) -> None:
    assert (this == other) is result


def test_records_set() -> None:
    data = [PageStatisticsRecord(
        project='project', article='article', granularity=DateGranularity.Daily,
        timestamp=dt.date(2025, 1, 1), access=AccessType.Any,
        agent=UserAgent.Any, views=10
    )] * 100
    assert len(list(set(data))) == 1


def test_page_statistics_init_success() -> None:
    data = [PageStatisticsRecord(
        project='project', article='article', granularity=DateGranularity.Daily,
        timestamp=dt.date(2025, 1, 1) + dt.timedelta(days=i),
        access=AccessType.Any, agent=UserAgent.Any, views=10
    ) for i in range(10)]

    page = PageStatistics(*data)
    assert isinstance(page, PageStatistics)
    assert page.project == 'project'
    assert page.article == 'article'
    assert len(page.records) == 10 == page.records_count
    assert page.start_date == dt.date(2025, 1, 1)
    assert page.end_date == dt.date(2025, 1, 10)
    assert page.total_views == 100
    assert page.url == 'https://project.org/wiki/article'

    assert isinstance(page.to_df(), pd.DataFrame)
    assert page.to_df().shape[0] == 10


@pytest.mark.parametrize(('url', 'project', 'article'), [
    ('https://ru.wikipedia.org/wiki/article', 'ru.wikipedia', 'article'),
    ('https://en.wikipedia.org/wiki/another_article,yes', 'en.wikipedia', 'another_article,yes')
])
def test_request_parse_url(url: str, project: str, article: str) -> None:
    resulting_project, resulting_article = WikimediaRequest(url, dt.date(2025, 1, 1), dt.date(2025, 1, 12))._parse_url()
    assert resulting_project == project
    assert resulting_article == article


@pytest.mark.parametrize('granularity', [g for g in DateGranularity])
@pytest.mark.parametrize('access', [a for a in AccessType])
@pytest.mark.parametrize('agent', [a for a in UserAgent])
@pytest.mark.parametrize(('start', 'end'), [
    (dt.date(2025, 1, 1), dt.date(2025, 1, 10)),
    (dt.date(2025, 1, 10), dt.date(2025, 1, 1)),
], ids=lambda x: x.strftime('%Y%m%d'))
def test_request_to_url(granularity: DateGranularity, access: AccessType, agent: UserAgent,
                        start: dt.date, end: dt.date) -> None:
    request = WikimediaRequest(
        url='https://ru.wikipedia.org/wiki/article-yes',
        granularity=granularity,
        access=access,
        agent=agent,
        start_timestamp=start,
        end_timestamp=end
    )
    expected_url = f'/ru.wikipedia/{access.value}/{agent.value}/article-yes/{granularity.value}/2025010100/2025011000'
    assert expected_url == request.as_url
