"""Script to fetch data from temples.ru."""
import asyncio
import dataclasses
import json
import re
import sys
import urllib.parse
from typing import (AsyncIterator, Dict, List, Optional, Set,
                    TextIO, Tuple, Union)

import aiohttp
import click
from lxml.etree import Element
from lxml.html.soupparser import fromstring as soup_parse

from utils import (aiohttp_get, lxml_get_link_data, iter_element_text_objects,
                   strip_advanced)

TEMPLES_ROOT_URL = 'http://www.temples.ru'
TEMPLES_TREE_URL = TEMPLES_ROOT_URL + '/tree.php'
TEMPLES_BRANCH_URL = TEMPLES_ROOT_URL + '/branch.php'


DictList = List[Dict[str, str]]
DictList1 = List[Dict[str, Union[str, DictList]]]
DictList2 = List[Dict[str, Union[str, DictList1]]]


@dataclasses.dataclass
class TempleCounter:
    """Counter of fetched temples."""

    display_interval: int
    temple_count: int = 0

    def increment(self) -> None:
        """Increment count."""
        self.temple_count += 1
        if (self.temple_count % self.display_interval) == 0:
            click.echo(f'Processed temples: {self.temple_count}')


async def get_region_ids(
    session: aiohttp.ClientSession
) -> List[Tuple[int, str]]:
    """Return list of region and IDs and names."""
    url = TEMPLES_TREE_URL

    r1 = await aiohttp_get(
        session,
        url,
        {
            'ID': 0
        }
    )

    if r1.status != 200:
        raise ValueError('Status code is {}'.format(r1.status))

    html = await r1.text()

    links = soup_parse(html).cssselect('a.Locate')
    if not isinstance(links, list):
        links = list(links)

    result: List[Tuple[int, str]] = []
    for link in links:
        link_data = lxml_get_link_data(link)
        if link_data is None:
            continue
        link_url, link_title = link_data
        query = urllib.parse.parse_qs(urllib.parse.urlparse(link_url).query)
        result.append((int(query['ID'][0]), link_title))

    return result


TempleData = Dict[
    str,
    Union[
        int, str, None, Dict[str, str],
        Dict[str, Union[str, Dict[str, float], List[str]]],
        Dict[str, List[str]],
        List[str]
    ]
]


@dataclasses.dataclass
class Temple:
    """Search result item."""

    temple_id: int
    name: str
    town: str
    construction_date: str
    url: Optional[str]
    card_data: Optional[Dict[str, List[str]]] = None
    card_name: Optional[str] = None
    card_name_synonyms: Optional[List[str]] = None
    card_slang_names: Optional[List[str]] = None
    card_type: Optional[str] = None
    card_construction_date: Optional[str] = None
    card_last_building_construction_date: Optional[str] = None
    card_architect: Optional[str] = None
    card_architects: Optional[List[str]] = None
    card_main_publication: Optional[str] = None
    card_historical_religion: Optional[str] = None
    card_current_religion: Optional[str] = None
    card_status: Optional[str] = None
    card_address: Optional[str] = None
    card_address_1917: Optional[str] = None
    card_description: Optional[str] = None
    card_notes: Optional[str] = None
    card_altar: Optional[str] = None
    card_altars: Optional[List[str]] = None
    card_web_url: Optional[str] = None
    card_email: Optional[str] = None
    card_phone: Optional[str] = None
    card_dedication: Optional[str] = None
    card_location: Optional[Tuple[float, float]] = None
    card_meta_date: Optional[str] = None
    card_meta_update_date: Optional[str] = None
    card_meta_author: Optional[str] = None
    card_unparsed_field_names: Optional[Set[str]] = None

    def get_json_dict(self) -> TempleData:
        """Get dictionary representation for JSON."""
        result: TempleData = {
            'temple_id': self.temple_id,
            'name': self.name,
            'town': self.town,
            'construction_date': self.construction_date,
            'url': self.url,
            'card': self.card_data,
        }

        if self.card_unparsed_field_names:
            result['card_unparsed_field_names'] = list(
                self.card_unparsed_field_names
            )

        card_fields: Dict[str, Union[str, Dict[str, float], List[str]]] = {}
        excluded_fields = (
            'card_location', 'card_unparsed_field_names', 'card_data'
        )
        for field_name in dir(self):
            if field_name.startswith('card_'):
                if (field_name not in excluded_fields):
                    field_value = getattr(self, field_name)
                    if field_value is not None:
                        card_fields[field_name] = field_value
        if self.card_location is not None:
            card_fields['card_location'] = {
                'longitude': self.card_location[0],
                'latitude': self.card_location[1],
            }

        if len(card_fields):
            result['card_fields'] = card_fields

        return result

    def parse_location(
        self, element: Element
    ) -> Optional[Tuple[float, float]]:
        """Parse location field element."""
        if len(element) == 0:
            return None
        element1 = element[0]
        if len(element1) == 0:
            return None
        element2 = element1[0]
        if len(element2) < 2:
            return None
        element3 = element2[1]
        if len(element3) < 2:
            return None
        location_element = element3[1]
        location_match = re.match(
            r'([0-9]+\.[0-9]+)°N\s+([0-9]+\.[0-9]+)°E',
            location_element.text_content().strip()
        )
        if location_match is None:
            return None
        return (location_match.group(1), location_match.group(2))

    async def fetch_card(self, session: aiohttp.ClientSession) -> None:
        """Fetch card data using URL."""
        if self.url is None:
            return

        r1 = await aiohttp_get(
            session,
            self.url
        )

        if r1.status != 200:
            click.echo(
                'Status code for page {} is {}'.format(self.url, r1.status)
            )
            return

        html = await r1.text()

        tree = soup_parse(html)

        title = tree.find('.//title').text
        if title.strip() == 'Реестр храмов: объект не найден':
            click.echo(
                'Object for page {} is not found'
            )
            return

        tables = tree.cssselect(
            '.center-block > table:nth-of-type(4) > tr > td > table'
        )
        if len(tables) == 0:
            tables = tree.cssselect(
                '.center-block > table:nth-of-type(4) > tbody > tr > td > table'
            )
        rows = tables[0]
        card_data: Dict[str, List[str]] = {}
        card_unparsed_field_names: Set[str] = set()
        for row in rows[1:]:
            cells = list(row)
            field_name = strip_advanced(cells[0].text_content().strip())
            field_element = cells[1]
            field_texts = list(iter_element_text_objects(field_element))
            field_text = ' '.join(field_texts)
            card_data[field_name] = field_texts
            if field_name == 'Название':
                self.card_name = field_text
            elif field_name == 'Синонимы названия':
                self.card_name_synonyms = field_text.split('; ')
            elif field_name == 'Обиходные названия':
                self.card_slang_names = field_text.split('; ')
            elif field_name == 'Тип постройки':
                self.card_type = field_text
            elif field_name == 'Дата основания':
                self.card_construction_date = field_text
            elif field_name == 'Дата постройки последнего здания':
                self.card_last_building_construction_date = field_text
            elif field_name == 'Архитектор':
                self.card_architect = field_text
            elif field_name == 'Архитекторы':
                self.card_architects = field_texts
            elif field_name == 'Основная публикация':
                self.card_main_publication = field_text
            elif field_name == 'Историческое исповедание':
                self.card_historical_religion = field_text
            elif field_name == 'Современная принадлежность':
                self.card_current_religion = field_text
            elif field_name == 'Статус':
                self.card_status = field_text
            elif field_name == 'Современный адрес':
                self.card_address = field_text
            elif field_name == 'Адрес на 1917 г.':
                self.card_address_1917 = field_text
            elif field_name == 'Краткое описание':
                self.card_description = field_text
            elif field_name == 'Примечания':
                self.card_notes = field_text
            elif field_name == 'Престол':
                self.card_altar = field_text
            elif field_name == 'Престолы':
                self.card_altars = field_texts
            elif field_name == 'Телефон':
                self.card_phone = field_text
            elif field_name == 'Web':
                self.card_web_url = field_text
            elif field_name == 'E-mail':
                self.card_email = field_text
            elif field_name == 'Посвящение':
                self.card_dedication = field_text
            elif field_name == 'Дата создания карточки':
                self.card_meta_date = field_text
            elif field_name == 'Дата обновления карточки':
                self.card_meta_update_date = field_text
            elif field_name == 'Составитель':
                self.card_meta_author = field_text
            elif field_name == 'Местоположение':
                location = self.parse_location(field_element)
                if location is not None:
                    self.card_location = location
            else:
                card_unparsed_field_names.add(field_name)

        self.card_data = card_data
        self.card_unparsed_field_names = card_unparsed_field_names


async def iterate_temples(
    region_id: int, session: aiohttp.ClientSession
) -> AsyncIterator[Temple]:
    """Iterate over region temples."""
    url = TEMPLES_BRANCH_URL

    r1 = await aiohttp_get(
        session,
        url,
        {
            'BranchID': region_id
        }
    )

    if r1.status != 200:
        raise ValueError('Status code is {}'.format(r1.status))

    html = await r1.text()

    rows = soup_parse(html).cssselect('.center-block > table:last-of-type tr')
    for row in rows[3:-3]:
        cells = row
        name = cells[1].text_content().strip()
        link = cells[1].find('a')
        url = link.get('href')
        if url:
            if url[0] == '/':
                url = TEMPLES_ROOT_URL + url
        town = cells[2].text_content().strip()
        construction_date = cells[4].text_content().strip()
        temple_id = int(cells[7].text_content().strip(' []'))
        temple = Temple(temple_id, name, town, construction_date, url)
        await temple.fetch_card(session)
        yield temple


async def process_region(
    region: Tuple[int, str], session: aiohttp.ClientSession,
    counter: TempleCounter
) -> Tuple[str, Dict[str, Union[str, int, List[TempleData]]]]:
    """
    Fetch region temples data asynchronously.

    Return tuple of region name and data.
    """
    region_id, region_name = region
    temples: List[TempleData] = []
    async for temple in iterate_temples(region_id, session):
        temples.append(temple.get_json_dict())
        counter.increment()
    return (
        region_name, {
            'name': region_name,
            'id': region_id,
            'temples': temples
        }
    )


async def fetch_temples_data_internal(
    start_region_index: int, end_region_index: int,
    connection_limit: int, counter_display_interval: int
) -> Dict[str, Dict[str, Union[str, int, List[TempleData]]]]:
    """Fetch temples asynchronously."""
    connector = aiohttp.connector.TCPConnector(limit=connection_limit)
    counter = TempleCounter(counter_display_interval)

    async with aiohttp.ClientSession(connector=connector) as session:
        regions = list(await get_region_ids(session))
        data: Dict[str, Dict[str, Union[str, int, List[TempleData]]]] = {}
        for region_name, region_data in await asyncio.gather(
            *map(
                lambda region: process_region(region, session, counter),
                regions[start_region_index:end_region_index]
            )
        ):
            data[region_name] = region_data

    return data


@click.command()
@click.option(
    '--output-file', type=click.File(mode='wt')
)
@click.option(
    '--start-region-index', type=click.IntRange(min=0),
    default=0,
    help='Region number to start with (inclusively)'
)
@click.option(
    '--end-region-index', type=click.IntRange(min=0),
    help='Region number to end with (exclusively)'
)
@click.option(
    '--connection-limit', type=click.IntRange(min=1),
    default=25,
    help='Maximum simultaneous connection count'
)
@click.option(
    '--counter-display-interval', type=click.IntRange(min=1),
    default=5,
    help='Interval to display temples counter (for example, each 10 temples)'
)
def fetch_temples_data(
    output_file: Optional[TextIO],
    start_region_index: int, end_region_index: int,
    connection_limit: int, counter_display_interval: int
) -> None:
    """Get data about temples and write it to JSON file."""
    data = asyncio.run(fetch_temples_data_internal(
        start_region_index, end_region_index, connection_limit,
        counter_display_interval
    ))

    if output_file is None:
        output_file = sys.stdout
    json.dump(data, output_file, ensure_ascii=False, indent=4)
