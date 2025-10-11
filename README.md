# 🌐 Wikipedia Parser

Simple project to collect data from Wikipedia projects. Currently, only Wikimedia page statistics is available.
Other parsers are coming, probably, soon... 

## Installation

Using pip:

```bash
pip install git+https://github.com/loginchik/wikipedia-parser@latest 
```

Using poetry:

```bash
poetry add git+https://github.com/loginchik/wikipedia-parser@latest 
```

Using uv:

```bash
uv add git+https://github.com/loginchik/wikipedia-parser@latest 
```

## Usage

### Wikimedia parser

Using [Wikimedia parser](src/wikimedia_parser), you can request statistics of page views. The main executable object 
inherits from [WikimediaParser](src/wikimedia_parser/parser.py) class, while other [types](src/wikimedia_parser/types.py) 
and [enums](src/wikimedia_parser/enums.py) are used to construct a request object and to process collected data. 

To gather statistics on 1 page, it is possible to manually construct a request object and to pass it to parser:

```python
import pandas as pd
from datetime import date
from typing import TYPE_CHECKING

from wikimedia_parser import (
    WikimediaParser,
    WikimediaRequest
)
from wikimedia_parser.enums import (
    UserAgent,
    AccessType,
    DateGranularity
)

if TYPE_CHECKING:
    import pandas

    ad
    pd

    from wikimedia_parser.types import PageStatistics

# Construct request object 
request = WikimediaRequest(
    url="https://en.wikipedia.org/wiki/Wikipedia",
    start_timestamp=date(2024, 1, 1),
    end_timestamp=date(2024, 12, 31),
    agent=UserAgent.User,  # optional; check default value
    access=AccessType.Any,  # optional; check default value
    granularity=DateGranularity.Daily,  # optional; check default value
)

# Create parser instance 
parser = WikimediaParser(
    timeout=60,  # optional; check default value
    max_connections=10  # optional; check default value
)

# Gather data 
page_data: PageStatistics = await parser.get_page_statistics(request=request)

# It is possible to export collected data to pandas DataFrame
df: pd.DataFrame = page_data.to_df()
```

To obtain multiple pages, it is recommended to use another method that implements `asyncio.gather` 
to speed up data loading. It is important to limit maximum number of `https.AsyncClient` connections 
with the `max_connections` attribute of a parser object. The same method can be used to gather a single page, 
as it implements requests compilation. 

Example:

```python
from datetime import date 
from typing import TYPE_CHECKING, List

from wikimedia_parser import WikimediaParser

if TYPE_CHECKING:
    import pandas as pd
    
    from wikimedia_parser.types import PageStatistics


# Create parser instance 
parser = WikimediaParser(
    timeout=60,  # optional; check default value
    max_connections=10  # optional; check default value
)

# Gather data 
pages_data: List[PageStatistics] = await parser.get_multiple_pages_statistics(
    pages=[
        "https://en.wikipedia.org/wiki/Wikipedia",
        "https://en.wikipedia.org/wiki/Russian_Wikipedia",
        "https://en.wikipedia.org/wiki/German_Wikipedia"
    ],
    start_date=date(2024, 1, 1), 
    end_date=date(2024, 12, 31), 
)

# The same parser can be used to concat pages' data into one DataFrame
global_df: pd.DataFrame = parser.concat_statistics(pages_data)
```
