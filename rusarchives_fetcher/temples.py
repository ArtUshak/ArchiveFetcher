"""Script to fetch data from temples.ru."""
import asyncio
import dataclasses
import itertools
import pathlib
import re
import urllib.parse
from typing import (Any, Awaitable, BinaryIO, Dict, Iterator, List, Optional,
                    Set, Tuple, Union)

import aiohttp
import click
import orjson
from lxml.etree import Element
from lxml.html.soupparser import fromstring as soup_parse

from utils import (aiohttp_get, generate_wiki_template_text,
                   lxml_get_link_data, lxml_iter_element_text_objects,
                   strip_advanced, trunc_str_bytes)

TEMPLES_ROOT_URL = 'http://www.temples.ru'
TEMPLES_TREE_URL = TEMPLES_ROOT_URL + '/tree.php'
TEMPLES_BRANCH_URL = TEMPLES_ROOT_URL + '/branch.php'


DictList = List[Dict[str, str]]
DictList1 = List[Dict[str, Union[str, DictList]]]
DictList2 = List[Dict[str, Union[str, DictList1]]]


@dataclasses.dataclass
class Counter:
    """Counter."""

    display_interval: int
    temple_count: int = 0

    def increment(self) -> None:
        """Increment count."""
        self.temple_count += 1
        if (self.temple_count % self.display_interval) == 0:
            click.echo(f'Processed count: {self.temple_count}')


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

    STR_FIELDS = ['name', 'town', 'construction_date']
    OPTIONAL_STR_FIELDS = ['url']
    OPTIONAL_CARD_STR_FIELDS = [
        'card_name', 'card_type', 'card_construction_date',
        'card_last_building_construction_date', 'card_architect',
        'card_main_publication', 'card_historical_religion',
        'card_current_religion', 'card_status', 'card_address',
        'card_address_1917', 'card_description', 'card_notes',
        'card_altar', 'card_web_url', 'card_email', 'card_phone',
        'card_dedication', 'card_meta_date', 'card_meta_update_date',
        'card_meta_author'
    ]
    OPTIONAL_CARD_LIST_STR_FIELDS = [
        'card_name_synonyms', 'card_slang_names', 'card_architects',
        'card_altars', 'card_hierarchy_old', 'card_hierarchy_modern'
    ]

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
    card_hierarchy_old: Optional[List[str]] = None
    card_hierarchy_modern: Optional[List[str]] = None

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

    @staticmethod
    def from_json_dict(data: Any) -> 'Temple':
        """Create from data loaded from JSON."""
        if not isinstance(data, dict):
            raise ValueError()
        kwargs: Dict[str, Any] = {}
        if 'temple_id' in data:
            if not isinstance(data['temple_id'], int):
                raise ValueError()
            kwargs['temple_id'] = data['temple_id']
        for field_name in Temple.STR_FIELDS:
            if field_name in data:
                if not isinstance(data[field_name], str):
                    raise ValueError()
                kwargs[field_name] = data[field_name]
            else:
                raise ValueError()
        for field_name in Temple.OPTIONAL_STR_FIELDS:
            if field_name in data:
                if not isinstance(data[field_name], str):
                    raise ValueError()
                kwargs[field_name] = data[field_name]
        if ('card' in data) and (data['card'] is not None):
            if not isinstance(data['card'], dict):
                raise ValueError()
            for key, value in data['card'].items():
                if not isinstance(key, str):
                    raise ValueError()
                if not isinstance(value, list):
                    raise ValueError()
                for element in value:
                    if not isinstance(element, str):
                        raise ValueError()
            kwargs['card_data'] = data['card']
        if 'card_unparsed_field_names' in data:
            if not isinstance(data['card_unparsed_field_names'], list):
                raise ValueError()
            for element in data['card_unparsed_field_names']:
                if not isinstance(element, str):
                    raise ValueError()
            kwargs['card_unparsed_field_names'] = set(
                data['card_unparsed_field_names']
            )
        if 'card_fields' in data:
            card_data = data['card_fields']
            if not isinstance(card_data, dict):
                raise ValueError
            for field_name in Temple.OPTIONAL_CARD_STR_FIELDS:
                if field_name in card_data:
                    if not isinstance(
                        card_data[field_name], str
                    ):
                        raise ValueError()
                    kwargs[field_name] = card_data[field_name]
            for field_name in Temple.OPTIONAL_CARD_LIST_STR_FIELDS:
                if field_name in card_data:
                    if not isinstance(
                        card_data[field_name], list
                    ):
                        raise ValueError()
                    for element in card_data[field_name]:
                        if not isinstance(element, str):
                            raise ValueError()
                    kwargs[field_name] = card_data[field_name]
            if 'card_location' in card_data:
                if not isinstance(card_data['card_location'], dict):
                    raise ValueError()
                if 'longitude' not in card_data['card_location']:
                    raise ValueError()
                longitude_raw = card_data['card_location']['longitude']
                longitude: float
                if isinstance(longitude_raw, str):
                    longitude = float(longitude_raw)
                elif isinstance(longitude_raw, float):
                    longitude = longitude_raw
                else:
                    raise ValueError()
                if 'latitude' not in card_data['card_location']:
                    raise ValueError()
                latitude_raw = card_data['card_location']['latitude']
                latitude: float
                if isinstance(latitude_raw, str):
                    latitude = float(latitude_raw)
                elif isinstance(latitude_raw, float):
                    latitude = latitude_raw
                else:
                    raise ValueError()
                kwargs['card_location'] = longitude, latitude
        return Temple(**kwargs)

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
        return float(location_match.group(1)), float(location_match.group(2))

    def parse_hierarchy(
        self, element: Element
    ) -> Optional[List[str]]:
        """Parse hierarchy element and return list (except root element)."""
        result = list(map(
            lambda table: table.text_content().strip(),
            element.cssselect('table')
        ))
        if not len(result):
            return None
        return result

    async def fetch_card(
        self, session: aiohttp.ClientSession, counter: Counter
    ) -> None:
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
            counter.increment()
            return

        html = await r1.text()

        tree = soup_parse(html)

        title = tree.find('.//title').text
        if title.strip() == 'Реестр храмов: объект не найден':
            click.echo(
                'Object for page {} is not found'.format(self.url)
            )
            counter.increment()
            return

        hierarchies = tree.cssselect(
            '.center-block > table:nth-of-type(1) > tr > td'
        )
        if len(hierarchies) >= 1:
            self.card_hierarchy_modern = self.parse_hierarchy(hierarchies[0])
        if len(hierarchies) >= 2:
            self.card_hierarchy_old = self.parse_hierarchy(hierarchies[1])

        tables = tree.cssselect(
            '.center-block > table:nth-of-type(4) > tr > td > table'
        )
        if len(tables) == 0:
            tables = tree.cssselect(
                '.center-block > table:nth-of-type(4) > tbody > tr > td >'
                ' table'
            )
        rows = tables[0]
        card_data: Dict[str, List[str]] = {}
        card_unparsed_field_names: Set[str] = set()
        for row in rows[1:]:
            cells = list(row)
            field_name = strip_advanced(cells[0].text_content().strip())
            field_element = cells[1]
            field_texts = list(lxml_iter_element_text_objects(field_element))
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
        counter.increment()

    def get_name(self) -> str:
        """Return temple name using `card_name` or `name` field."""
        return self.card_name or self.name

    def get_truncated_name(self, temple_prefix: str) -> str:
        """Return truncated name with ID."""
        return trunc_str_bytes(
            temple_prefix + self.get_name(), 200, om='...'
        ) + ' (' + str(self.temple_id) + ')'

    def get_page_name_modern(self) -> Optional[str]:
        """Return page name generated using modern hierarchy."""
        if self.card_hierarchy_modern is not None:
            return '/'.join(
                self.card_hierarchy_modern + [self.get_truncated_name()]
            )
        else:
            return None

    def get_page_name_old(self) -> Optional[str]:
        """Return page name generated using old (pre-1917) hierarchy."""
        if self.card_hierarchy_old is not None:
            return '/'.join(
                self.card_hierarchy_old + [self.get_truncated_name()]
            )
        else:
            return None

    def get_page_text(self) -> str:
        """Return generated page wikitext for temple."""
        template_parameters: Dict[str, str] = {}
        for field_name in self.STR_FIELDS:
            if field_name == 'construction_date':
                continue
            field_value = getattr(self, field_name)
            template_parameters[field_name] = field_value
        for field_name in self.OPTIONAL_STR_FIELDS:
            field_value = getattr(self, field_name)
            if field_value is not None:
                template_parameters[field_name] = field_value
        for field_name in self.OPTIONAL_CARD_STR_FIELDS:
            field_value = getattr(self, field_name)
            if field_value is not None:
                template_parameters[field_name] = field_value
        for field_name in self.OPTIONAL_CARD_LIST_STR_FIELDS:
            field_value = getattr(self, field_name)
            if field_value is not None:
                for i in range(len(field_value)):
                    template_parameters[field_name + str(i)] = field_value[i]
        if self.card_location is not None:
            template_parameters['longitude'] = str(self.card_location[0])
            template_parameters['latitude'] = str(self.card_location[1])

        return generate_wiki_template_text(
            'Храм', template_parameters
        )


class HierarchyIndex:
    """Hierarchical index of temples."""

    is_old: bool
    name: str
    strip_in_brackets: bool
    parent: Optional['HierarchyIndex'] = None
    child_temples: Dict[str, Temple] = {}
    child_indices: Dict[str, 'HierarchyIndex'] = {}

    def __init__(
        self, is_old: bool, name: str, strip_in_brackets: bool,
        parent: Optional['HierarchyIndex'] = None
    ):
        """Initialize."""
        self.is_old = is_old
        self.name = name
        self.parent = parent
        self.strip_in_brackets = strip_in_brackets
        self.child_temples = {}
        self.child_indices = {}

    def _add_temple_recursive(
        self, temple: Temple, hierarchy: List[str]
    ) -> None:
        if not len(hierarchy):
            name = temple.get_name()
            if name in self.child_temples:
                return  # TODO
            self.child_temples[name] = temple
        else:
            top_name = hierarchy[0]
            if self.strip_in_brackets:
                top_name = top_name.split(' (')[0].strip()
            index: HierarchyIndex
            if top_name in self.child_indices:
                index = self.child_indices[top_name]
            else:
                index = HierarchyIndex(
                    is_old=self.is_old,
                    name=top_name,
                    strip_in_brackets=self.strip_in_brackets,
                    parent=self
                )
                self.child_indices[top_name] = index
            index._add_temple_recursive(temple, hierarchy[1:])

    def add_temple(self, temple: Temple) -> None:
        """Add temple using old (pre-1917) or modern hiearchy."""
        hierarchy: Optional[List[str]]
        if self.is_old:
            hierarchy = temple.card_hierarchy_old
        else:
            hierarchy = temple.card_hierarchy_modern
        if hierarchy is None:
            return
        self._add_temple_recursive(temple, hierarchy)

    def get_page_text(
        self, prefix: str, temple_prefix: str
    ) -> str:
        """Return generated page wikitext for index."""
        template_parameters = {
            'old': str(int(self.is_old)),
            'name': self.name
        }
        result = generate_wiki_template_text(
            'Иерархия', template_parameters
        ) + '\n'
        if len(self.child_indices):
            result += '\n== Регионы ==\n\n' + '\n'.join(list(map(
                lambda child_index:
                f'* [[/{child_index.name}|{child_index.name}]]',
                self.child_indices.values()
            ))) + '\n'
        if len(self.child_temples):
            result += '\n== Храмы ==\n\n' + '\n'.join(list(map(
                lambda child_temple:
                '* [[' + child_temple.get_truncated_name(temple_prefix) + '|'
                + child_temple.get_name() + ']]',
                self.child_temples.values()
            ))) + '\n'
        return result

    def generate_hierarchy_pages(
        self, prefix: str, temple_prefix: str, generate_temples: bool
    ) -> Iterator[Tuple[str, str]]:
        """
        Iterate over tuple of page names and texts.

        Both subindices and temples are returned.
        """
        yield (
            prefix + self.name,
            self.get_page_text(prefix, temple_prefix)
        )
        new_prefix = prefix + self.name + '/'
        for temple in self.child_temples.values():
            if generate_temples:
                page_text = temple.get_page_text()
                if page_text is not None:
                    yield (
                        temple.get_truncated_name(temple_prefix),
                        page_text
                    )
        for child_index in self.child_indices.values():
            for page_name, page_text in child_index.generate_hierarchy_pages(
                new_prefix, temple_prefix, generate_temples
            ):
                yield page_name, page_text


async def list_temples(
    region_id: int, session: aiohttp.ClientSession, counter: Counter
) -> List[Temple]:
    """Return list of over region temples."""
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
    awaitables: List[Awaitable[None]] = []
    temples: List[Temple] = []

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
        temples.append(temple)
        awaitables.append(temple.fetch_card(session, counter))

    await asyncio.gather(*awaitables)
    return temples


async def process_region(
    region: Tuple[int, str], session: aiohttp.ClientSession,
    counter: Counter
) -> Tuple[str, Dict[str, Union[str, int, List[TempleData]]]]:
    """
    Fetch region temples data asynchronously.

    Return tuple of region name and data.
    """
    region_id, region_name = region
    temples: List[TempleData] = []
    for temple in await list_temples(region_id, session, counter):
        temples.append(temple.get_json_dict())
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
    counter = Counter(counter_display_interval)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout
    ) as session:
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
@click.argument(
    'output-file', type=click.File(mode='wb')
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
    default=20,
    help='Maximum simultaneous connection count'
)
@click.option(
    '--counter-display-interval', type=click.IntRange(min=1),
    default=5,
    help='Interval to display temples counter (for example, each 10 temples)'
)
def fetch_temples_data(
    output_file: BinaryIO,
    start_region_index: int, end_region_index: int,
    connection_limit: int, counter_display_interval: int
) -> None:
    """Get data about temples and write it to JSON file."""
    data = asyncio.run(fetch_temples_data_internal(
        start_region_index, end_region_index, connection_limit,
        counter_display_interval
    ))

    output_file.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))


@click.command()
@click.argument(
    'input-file',
    type=click.File(mode='rb')
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
@click.argument(
    'output-list-file',
    type=click.File(mode='wb')
)
@click.option(
    '--old-name', type=click.STRING,
    default='Храмы (до 1917)',
    help='Old hierarchy root name'
)
@click.option(
    '--modern-name', type=click.STRING,
    default='Храмы',
    help='Modern hiearchy root name'
)
@click.option(
    '--old-prefix', type=click.STRING,
    default='',
    help='Prefix for old hierarchy pages'
)
@click.option(
    '--modern-prefix', type=click.STRING,
    default='',
    help='Prefix for modern hierarchy pages'
)
@click.option(
    '--temple-prefix', type=click.STRING,
    default='',
    help='Prefix for temple pages'
)
@click.option(
    '--strip-in-brackets/--no-strip-in-brackets',
    default=True,
    help='Strip text starting with first bracket from hierarchy names'
)
def generate_temples_pages(
    input_file: BinaryIO, output_directory: str, output_list_file: BinaryIO,
    old_name: str, modern_name: str,
    old_prefix: str, modern_prefix: str, temple_prefix: str,
    strip_in_brackets: bool
) -> None:
    """Generate wiki-text pages for all temples."""
    output_directory_path = pathlib.Path(output_directory)
    data = orjson.loads(input_file.read())

    temples: List[Temple] = []
    for region_data in data.values():
        for element in region_data['temples']:
            temples.append(Temple.from_json_dict(element))

    modern_index = HierarchyIndex(
        is_old=False, name=modern_name, strip_in_brackets=strip_in_brackets
    )
    old_index = HierarchyIndex(
        is_old=True, name=old_name, strip_in_brackets=strip_in_brackets
    )
    with click.progressbar(temples, show_pos=True) as bar1:
        for temple in bar1:
            modern_index.add_temple(temple)
            old_index.add_temple(temple)

    page_files: Dict[str, str] = {}
    page_names: Set[str] = set()
    page_number = 0

    with click.progressbar(
        itertools.chain(
            modern_index.generate_hierarchy_pages(
                modern_prefix, temple_prefix, True
            ),
            old_index.generate_hierarchy_pages(
                old_prefix, temple_prefix, False
            )
        ),
        show_pos=True
    ) as bar2:
        for page_name, page_text in bar2:
            if page_name in page_names:
                raise ValueError(
                    f'Truncated page name {page_name} is already present'
                )
            if len(page_name.encode('utf-8')) > 255:
                raise ValueError(
                    f'Page name {page_name} is too long'
                )
            page_names.add(page_name)
            page_path = output_directory_path.joinpath(
                pathlib.Path(str(page_number)).with_suffix('.txt')
            )
            page_number += 1
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_files[str(page_path)] = page_name

            with open(page_path, mode='wt') as page_file:
                page_file.write(page_text)

    output_list_file.write(
        orjson.dumps(page_files, option=orjson.OPT_INDENT_2)
    )
