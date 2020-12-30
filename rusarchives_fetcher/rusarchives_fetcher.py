"""Script to fetch data from rusarchives.ru."""
import copy
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import sys
import time
import urllib.parse
from typing import (Any, Callable, ContextManager, Dict, Iterable, Iterator,
                    List, Optional, TextIO, Tuple, Union, cast)

import click
import pyexcel
import requests
import requests_html

ROOT_URL = 'http://rusarchives.ru'
ARCHIVE_LIST_LOCAL_URL = '/state/list'
SEARCH_URL = 'http://cfc.rusarchives.ru/CFC-search/'
SEARCH_PRELIMINARY_URL = (
    'http://cfc.rusarchives.ru/CFC-search/Search/GetSearchPrelimaryData2'
)
SEARCH_DETAILS_URL = (
    'http://cfc.rusarchives.ru/CFC-search/Search/DetailsModal'
)

MAX_TRY_NUM: int = 10
SLEEP_TIME_DEFAULT: float = 0.025
SLEEP_TIME_DISCONNECTED: float = 1.0


def strip_advanced(s: str) -> str:
    """Remove newlines and multiple whitespaces."""
    return re.sub(r'\s{2,}', ' ', s.replace('\n', ' '))


def get_number_str(x: Optional[int]) -> str:
    """Get string from number or empty string if number is `None`."""
    if x is None:
        return ''
    return str(x)


def get_str_number(s: str) -> Optional[int]:
    """Get number from string or `None` on empty string."""
    if not s:
        return None
    if s == 'null':
        return None
    if s == '#VALUE!':
        return None
    return int(s)


def request_get(
    session: requests_html.HTMLSession, url: str,
    params: Optional[Dict[str, Any]] = None
) -> requests_html.HTMLResponse:
    """Perform GET request and return HTTP response. Retry on error."""
    for _ in range(MAX_TRY_NUM):
        try:
            response: requests_html.HTMLResponse = session.get(
                url, params=params
            )
            time.sleep(SLEEP_TIME_DEFAULT)
            return response
        except requests.ConnectionError:
            time.sleep(SLEEP_TIME_DISCONNECTED)
    raise ValueError('Max request try num exceeded')  # TODO


def request_post(
    session: requests_html.HTMLSession, url: str,
    params: Optional[Dict[str, Any]] = None
) -> requests_html.HTMLResponse:
    """Perform GET request and return HTTP response. Retry on error."""
    for _ in range(MAX_TRY_NUM):
        try:
            response: requests_html.HTMLResponse = session.post(
                url, params=params
            )
            time.sleep(SLEEP_TIME_DEFAULT)
            return response
        except requests.ConnectionError:
            time.sleep(SLEEP_TIME_DISCONNECTED)
    raise ValueError('Max request try num exceeded')  # TODO


def get_link_data(
    link_element: requests_html.Element
) -> Optional[Tuple[str, str]]:
    """Return tuple of hyperlink URL and text if element is hyperlink."""
    try:
        href = link_element.attrs['href'].strip()
        if (
            href and not (href.startswith('#'))
            and not href.startswith(('javascript:', 'mailto:'))
        ):
            return href, link_element.full_text
        else:
            return None
    except KeyError:
        return None


def iterate_sections_links(
    local_url: str, session: requests_html.HTMLSession
) -> Iterator[Tuple[str, List[Tuple[str, str]]]]:
    """Iterate over content sections with link URLs and texts."""
    url = ROOT_URL + local_url

    r1 = request_get(
        session,
        url
    )

    if r1.status_code != 200:
        raise ValueError('Status code is {}'.format(r1.status_code))

    sections = r1.html.find('.views-table')
    if not isinstance(sections, list):
        sections = list(sections)
    for section in sections:
        section_heading = section.find('caption')
        if not isinstance(section_heading, requests_html.Element):
            section_heading = section_heading[0]
        section_heading_text = section_heading.full_text
        section_links: List[Tuple[str, str]] = []
        for link in section.find('a'):
            link_data = get_link_data(link)
            if link_data:
                section_links.append(link_data)
        yield section_heading_text, section_links


def get_field_data(
    field: requests_html.Element
) -> Optional[Dict[str, Union[str, List[str]]]]:
    """Return data for organization data field."""
    subelement_children_lxml = list(list(field.lxml)[0])

    if len(subelement_children_lxml) == 0:
        return None

    result_data: Dict[str, Union[str, List[str]]] = {}
    text_items: List[Optional[str]] = []
    if (
        len(subelement_children_lxml) == 1
    ):
        subelement_child_lxml = subelement_children_lxml[0]
        classes: List[str] = list(subelement_child_lxml.classes)
        if 'entity-field-collection-item' in classes:
            type_classes = list(filter(
                lambda class_name: class_name.startswith(
                    'field-collection-item-field-'
                ),
                classes
            ))
            result_data['type_classes'] = type_classes
            for node in subelement_child_lxml:
                if node.text:
                    text_items.append(node.text)
                    text_items.append(node.tail)
                for subnode in node:
                    text_items.append(subnode.text_content())
                    text_items.append(subnode.tail)
            text = '\n'.join(map(
                lambda s: s.strip(),
                cast(List[str], filter(bool, text_items))
            ))
            result_data['text'] = text
            return result_data

    text_items = list(map(
        lambda node: cast(Optional[str], node.text_content()),
        subelement_children_lxml
    ))
    text = '\n'.join(map(
        lambda s: s.strip(),
        cast(List[str], filter(bool, text_items))
    ))
    result_data['text'] = text

    return result_data


def get_organization_data(
    local_url: str, session: requests_html.HTMLSession
) -> List[Dict[str, Union[str, List[str]]]]:
    """Get data about archive organization."""
    url = ROOT_URL + local_url

    r1 = request_get(
        session,
        url
    )

    if r1.status_code != 200:
        raise ValueError('Status code is {}'.format(r1.status_code))

    field_groups: List[Dict[str, Union[str, List[str]]]] = []
    field_group_elements = r1.html.find('.field-group-htab')
    for field_group_element in field_group_elements:
        field_group_data = {}
        legend_element = field_group_element.find('legend')
        if not isinstance(legend_element, requests_html.Element):
            legend_element = legend_element[0]
        field_group_title = legend_element.full_text
        field_group_data['title'] = field_group_title
        field_elements = field_group_element.find('.field-item')
        fields = []
        for field_element in field_elements:
            field_data = get_field_data(field_element)
            fields.append(field_data)
        field_group_data['fields'] = fields
        field_groups.append(field_group_data)

    return field_groups


def get_search_result_data(
    data_id: str, data_kind: str, session: requests_html.HTMLSession
) -> List[Dict[str, Union[str, List[str]]]]:
    """Get data about search result."""
    url = SEARCH_DETAILS_URL

    params = {
        'ID': data_id,
        'Kind': data_kind
    }

    r1 = request_get(
        session,
        url,
        params
    )

    # Remove vertical tabs to avoid beatiful soup issues
    r1._content = r1.content.decode('utf-8').replace('\v', '').encode('utf-8')

    if r1.status_code == 500:
        title_text = r1.html.find('title')[0].full_text
        return [
            {
                'error': title_text
            }
        ]

    if r1.status_code != 200:
        raise ValueError(
            'Status code is {}, body is {}'.format(r1.status_code, r1.text)
        )

    text_model_elements = r1.html.find('.textModal')
    text_model_element_children_lxml = list(list(
        text_model_elements[0].lxml
    )[0])

    result: List[Dict[str, Union[str, List[str]]]] = []

    current_section_title: Optional[str] = None
    current_section_content: List[str] = []
    for element_lxml in text_model_element_children_lxml:
        if element_lxml.tag == 'h5':
            if (
                current_section_title
                or current_section_content
            ):
                section_data: Dict[str, Union[str, List[str]]] = {}
                if current_section_title:
                    section_data['title'] = current_section_title
                section_data['content'] = current_section_content
                result.append(section_data)
            current_section_title = element_lxml.text_content().strip(
                ' \t\r\n:'
            )
            current_section_content = []
        else:
            current_section_content.append(
                element_lxml.text_content().strip().replace('\r\n', '\n')
            )

    return result


class SearchResults:
    """Iterable class to search result data: IDs and kinds."""

    search_query: str
    url: str
    session: requests_html.HTMLSession
    params: Dict[str, Union[str, bool, int]]
    item_count: int

    def __init__(
        self, search_query: str, session: requests_html.HTMLSession
    ) -> None:
        """Initialize."""
        self.search_query = search_query
        self.session = session

        initial_params = {
            'searchString': search_query,
            'type': 'InUnitName'
        }
        r0 = request_post(
            self.session,
            SEARCH_PRELIMINARY_URL,
            initial_params
        )
        self.item_count = r0.json()['Data']['Count']

        self.params = {
            'PrewSearch': search_query,
            'SearchString': search_query,
            'Subject': '',
            'ArchiveInstitution': '',
            'ArchiveName': '',
            'FundPrefix': '',
            'FundNumb': '',
            'FundLetter': '',
            'InFundName': False,
            'InFundAnnotate': False,
            'InFundHistory': False,
            'InInventoryName': False,
            'InInventoryAnnotate': False,
            'InUnitName': False,
            'DetailsViewKey': 'InUnitName',
            'AllResultsOfType': self.item_count
        }

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        """Iterate over search results."""
        params = copy.copy(self.params)
        page_count: Optional[int] = None
        page_index = 0

        while (page_count is None) or (page_index < page_count):
            params['PageNumb'] = page_index + 1

            r1 = request_post(
                self.session,
                SEARCH_URL,
                params
            )
            if r1.status_code != 200:
                raise ValueError('Status code is {}'.format(r1.status_code))

            if page_count is None:
                page_count = int(r1.html.find('.pageBtnN')[0].text)

            detail_button_elements = r1.html.find('.openDetails')
            for element in detail_button_elements:
                data_id = element.attrs.get('dataid', None)
                data_kind = element.attrs.get('datakind', None)
                if (data_id is not None) and (data_kind is not None):
                    yield str(data_id), str(data_kind)

            page_index += 1

    def __len__(self) -> int:
        """Get search result count."""
        return self.item_count


@click.group()
def cli() -> None:
    """Run command line."""
    pass


DictList = List[Dict[str, str]]
DictList1 = List[Dict[str, Union[str, DictList]]]
DictList2 = List[Dict[str, Union[str, DictList1]]]


@click.command()
@click.option(
    '--output-file', type=click.File(mode='wt')
)
def list_organizations(
    output_file: Optional[TextIO]
) -> None:
    """Get list of archive organizations and write it to JSON file."""
    session = requests_html.HTMLSession()

    data: List[Dict[str, Union[str, DictList2]]] = []
    for section_title, section_links in iterate_sections_links(
        ARCHIVE_LIST_LOCAL_URL, session
    ):
        section_data: Dict[str, Union[str, DictList2]] = {}
        section_data['title'] = section_title
        section_data_links: DictList2 = []
        click.echo(f'Section {section_title}')
        for link_url, link_text in section_links:
            click.echo(f'Link {link_text} to {link_url}')
            link_data: Dict[
                str, Union[str, DictList1]
            ] = {}
            link_data['title'] = link_text
            link_data['url'] = link_url
            link_data_target_sections: List[
                Dict[str, Union[str, DictList]]
            ] = []

            for subsection_title, subsection_links in iterate_sections_links(
                link_url, session
            ):
                subsection_data: Dict[str, Union[str, DictList]] = {}
                subsection_data['title'] = subsection_title
                subsection_data_links: DictList = []

                for sublink_url, sublink_text in subsection_links:
                    sublink_data: Dict[str, str] = {}
                    sublink_data['title'] = sublink_text
                    sublink_data['url'] = sublink_url

                    subsection_data_links.append(sublink_data)

                subsection_data['links'] = subsection_data_links
                link_data_target_sections.append(subsection_data)

            link_data['target_sections'] = link_data_target_sections
            section_data_links.append(link_data)
        section_data['links'] = section_data_links
        data.append(section_data)

    if output_file is None:
        output_file = sys.stdout
    json.dump(data, output_file, ensure_ascii=False, indent=4)


@click.command()
@click.option(
    '--output-file', type=click.File(mode='wt')
)
@click.argument(
    'local-url', type=click.STRING
)
def fetch_organization_data(
    local_url: str, output_file: Optional[TextIO]
) -> None:
    """Get data about archive organization and write it to JSON file."""
    session = requests_html.HTMLSession()
    data = get_organization_data(local_url, session)

    if output_file is None:
        output_file = sys.stdout
    json.dump(data, output_file, ensure_ascii=False, indent=4)


@click.command()
@click.argument(
    'query', type=click.STRING
)
@click.option(
    '--output-file', type=click.File(mode='wt')
)
def list_search_results(
    query: str, output_file: Optional[TextIO]
) -> None:
    """Get list of search results and write it to JSON file."""
    session = requests_html.HTMLSession()

    search = SearchResults(query, session)
    with click.progressbar(search, show_pos=True) as progress_bar:
        data = list(map(
            lambda result: {'id': result[0], 'kind': result[1]},
            progress_bar
        ))

    if output_file is None:
        output_file = sys.stdout
    json.dump(data, output_file, ensure_ascii=False, indent=4)


@click.command()
@click.option(
    '--first-item', type=click.INT
)
@click.option(
    '--item-count', type=click.INT
)
@click.argument(
    'input-file', type=click.File(mode='rt')
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
def fetch_search_results(
    first_item: Optional[int], item_count: Optional[int],
    input_file: TextIO, output_directory: str
) -> None:
    """Get data about search results and write it to JSON file."""
    data = json.load(input_file)
    if first_item is not None:
        data = data[first_item:]
    if item_count is not None:
        data = data[:item_count]

    session = requests_html.HTMLSession()
    output_directory_path = pathlib.Path(output_directory)
    output_directory_path.mkdir()

    with click.progressbar(data, show_pos=True) as progress_bar:
        for item in progress_bar:
            data_id = item['id']
            data_kind = item['kind']

            search_result = get_search_result_data(data_id, data_kind, session)

            output_file_path = output_directory_path.joinpath(
                f'{data_id}_{data_kind}.json'
            )
            with open(output_file_path, 'wt') as output_file:
                json.dump(
                    search_result, output_file, ensure_ascii=False, indent=4
                )


def process_archive_title(title: str) -> str:
    """Convert archive name."""
    title1 = strip_advanced(title).strip()
    title2 = re.sub(r'«|»', '"', title1)
    title3 = title2.replace('администраци', 'Администраци')
    title4 = title3.replace('учереждение', 'учреждение')
    title5 = re.sub(r'(К|к)азеное', r'\1азённое', title4)
    title6 = re.sub(r'(К|к)азенное', r'\1азённое', title5)
    title7 = re.sub(r'област$', 'области', title6)
    title8 = re.sub(
        (
            r'^((|государственное |государственное областное '
            r'|государственное краевое |краевое государственное '
            r'|муниципальное |областное |областное государственное '
            r'|республиканское |республиканское государственное |федеральное )'
            r'(|бюджетное |каз(е|ё)нное |каз(е|ё)нное архивное )учреждение'
            r'(( [а-я]+ области| [а-я -]+ автономного округа – Югры| '
            r'республик (Карелия|Саха \(Якутия\)|Хакасия))?|)|ГУ|ГКУ|МКУ)'
        ),
        '',
        title7,
        flags=re.IGNORECASE
    )
    return title8.replace('"', '').strip()


def process_annotation(annotation: Optional[str]) -> Optional[str]:
    """Convert annotation text."""
    if not annotation:
        return None
    annotation = annotation.strip()
    return annotation[0].upper() + annotation[1:]


ItemData = List[Dict[str, Union[str, List[str]]]]


@dataclasses.dataclass
class Item:
    """Search result item."""

    parent: Optional['Inventory']
    item_number: Optional[str]
    item_annotation: Optional[str]
    data: ItemData
    start_year: Optional[int]
    end_year: Optional[int]
    url: Optional[str]

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'item_number': self.item_number,
            'item_annotation': self.item_annotation,
            'data': self.data,
            'start_year': self.start_year,
            'end_year': self.end_year,
            'url': self.url,
        }

    @staticmethod
    def from_json_dict(parent: 'Inventory', data: Any) -> 'Item':
        """Create from data loaded from JSON."""
        item_number = data['item_number']
        if isinstance(item_number, int):
            item_number = str(item_number)
        return Item(
            parent, item_number, data['item_annotation'], data['data'],
            data.get('start_year', None), data.get('end_year', None),
            data.get('url', None)
        )

    def get_number_str(self) -> str:
        """Return string representation of number."""
        return self.item_number or ''

    def get_page_text(self) -> str:
        """Return page wikitext for item."""
        if self.parent is None:
            raise ValueError()
        return (
            '{{ЕдиницаХранения|archive='
            + self.parent.parent.parent.get_title_str()
            + '|fund=' + self.parent.parent.get_number_str()
            + '|inventory=' + self.parent.get_number_str()
            + '|item=' + self.get_number_str()
            + '|fund_annotation=' + (self.parent.parent.annotation or '')
            + '|inventory_annotation=' + (self.parent.annotation or '')
            + '|item_annotation=' + (self.item_annotation or '')
            + '|start_year=' + get_number_str(self.start_year)
            + '|end_year=' + get_number_str(self.end_year)
            + '|url=' + (self.url or '') + '}}\n\n'
        )


@dataclasses.dataclass
class FullItem:
    """Search result item with data of archive, fund and inventory."""

    item: Item
    archive_title: Optional[str]
    fund_number: Optional[int]
    inventory_number: Optional[int]
    fund_annotation: Optional[str]
    inventory_annotation: Optional[str]


@dataclasses.dataclass
class Inventory:
    """Inventory data with items."""

    parent: 'Fund'
    number: Optional[int]
    items: Dict[Optional[str], Item]
    annotation: Optional[str]

    def append(self, item: FullItem) -> None:
        """Add item."""
        self.items[item.item.item_number] = item.item

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'number': self.number,
            'annotation': self.annotation,
            'items': {
                item_number: item.get_json_dict()
                for item_number, item in self.items.items()
            }
        }

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without items."""
        return {
            'number': self.number,
            'annotation': self.annotation
        }

    @staticmethod
    def from_json_dict(parent: 'Fund', data: Any) -> 'Inventory':
        """Create from data loaded from JSON."""
        result = Inventory(
            parent, data['number'], {}, data['annotation']
        )
        for item_number_str, item in data['items'].items():
            result.items[
                item_number_str
            ] = Item.from_json_dict(result, item)
        return result

    def get_number_str(self) -> str:
        """Return string representation of number."""
        return get_number_str(self.number)

    def get_item_page_text(
        self, item_number: Optional[str], separate: bool, heading_level: int
    ) -> str:
        """Return page wikitext for item in inventory."""
        heading_str = '=' * heading_level
        item = self.items[item_number]
        if separate:
            return (
                '{{СсылкаНаЕдиницуХранения|archive='
                + self.parent.parent.get_title_str()
                + '|fund=' + self.parent.get_number_str()
                + '|inventory=' + self.get_number_str()
                + '|item=' + (item_number or '') + '}}'
            )
        else:
            return (
                heading_str + ' Единица хранения ' + item.get_number_str()
                + ' ' + heading_str + '\n\n' + item.get_page_text()
            )

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for inventory."""
        heading_str = '=' * heading_level
        return (
            '{{Опись|archive=' + self.parent.parent.get_title_str()
            + '|fund=' + self.parent.get_number_str()
            + '|inventory=' + self.get_number_str()
            + '|fund_annotation=' + (self.parent.annotation or '')
            + '|inventory_annotation=' + (self.annotation or '') + '}}\n\n'
            + heading_str + ' Единицы хранения ' + heading_str + '\n\n'
            + '\n\n'.join(list(map(
                lambda item_number:
                self.get_item_page_text(
                    item_number, separate, heading_level + 1
                ),
                sorted(self.items.keys(), key=lambda k: k or -1)
            ))) + '\n'
        )


@dataclasses.dataclass
class InventoryLink:
    """Link to inventory, it may be stored in file and fetched."""

    parent: 'Fund'
    number: Optional[int]
    loaded_inventory: Optional[Inventory]

    @property
    def base_directory_path(self) -> Optional[pathlib.Path]:
        """Base directory path for all hierarchy."""
        return self.parent.base_directory_path

    def fetch(self) -> Inventory:
        """Return inventory, load from file if necessary."""
        if self.loaded_inventory is not None:
            return self.loaded_inventory

        if self.base_directory_path is None:
            raise ValueError()
        inventory_file_path = self.base_directory_path.joinpath(
            'archive' + self.parent.parent.get_title_hash()
        ).joinpath(
            'fund' + self.parent.get_number_str()
        ).joinpath(
            'inventory' + self.get_number_str() + '.json'
        )
        with open(inventory_file_path, 'rt') as inventory_file:
            data = json.load(inventory_file)
        self.loaded_inventory = Inventory.from_json_dict(self.parent, data)

        return self.loaded_inventory

    @property
    def inventory(self) -> Inventory:
        """Inventory object."""
        return self.fetch()

    def get_number_str(self) -> str:
        """Return string representation of number."""
        return get_number_str(self.number)

    @property
    def annotation(self) -> Optional[str]:
        """Inventory annotation."""
        return self.inventory.annotation

    @annotation.setter
    def annotation(self, value: Optional[str]) -> None:
        """Inventory annotation."""
        self.inventory.annotation = None

    def append(self, item: FullItem) -> None:
        """Add item."""
        return self.inventory.append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return self.inventory.get_json_dict()

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for inventory."""
        return self.inventory.get_page_text(separate, heading_level)

    @property
    def items(self) -> Dict[Optional[str], Item]:
        """Inventory items."""
        return self.inventory.items


@dataclasses.dataclass
class Fund:
    """Fund data with inventories."""

    parent: 'Archive'
    number: Optional[int]
    inventories: Dict[Optional[int], InventoryLink]
    annotation: Optional[str]

    @property
    def base_directory_path(self) -> Optional[pathlib.Path]:
        """Base directory path for all hierarchy."""
        return self.parent.base_directory_path

    def append(self, item: FullItem) -> None:
        """Add item."""
        if item.inventory_number not in self.inventories:
            self.inventories[item.inventory_number] = InventoryLink(
                self,
                item.inventory_number,
                Inventory(
                    self, item.inventory_number, {}, item.inventory_annotation
                )
            )
        inventory = self.inventories[item.inventory_number]
        if item.inventory_annotation:
            inventory.annotation = item.inventory_annotation
        inventory.append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'number': self.number,
            'annotation': self.annotation,
            'inventories':
            {
                get_number_str(inventory_number): None
                for inventory_number in self.inventories
            }
        }

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without inventories."""
        return {
            'number': self.number,
            'annotation': self.annotation
        }

    @staticmethod
    def from_json_dict(parent: 'Archive', data: Any) -> 'Fund':
        """Create from data loaded from JSON."""
        result = Fund(
            parent, data['number'], {}, data['annotation']
        )
        for inventory_number_str, inventory in data['inventories'].items():
            inventory_number = get_str_number(inventory_number_str)
            if inventory is None:
                result.inventories[inventory_number_str] = InventoryLink(
                    result, inventory_number, None
                )
            else:
                result.inventories[inventory_number_str] = InventoryLink(
                    result, inventory_number,
                    Inventory.from_json_dict(result, inventory)
                )
        return result

    def get_number_str(self) -> str:
        """Return string representation of number."""
        return get_number_str(self.number)

    def get_inventory_link_page_text(
        self, inventory_number: Optional[int], separate: bool,
        heading_level: int
    ) -> str:
        """Return page wikitext for inventory in fund."""
        if separate:
            return (
                '{{СсылкаНаОпись|archive=' + self.parent.get_title_str()
                + '|fund=' + self.get_number_str()
                + '|inventory=' + get_number_str(inventory_number) + '}}'
            )
        else:
            heading_str = '=' * heading_level
            inventory_title: str
            if inventory_number is None:
                inventory_title = 'Неизвестная опись'
            else:
                inventory_title = 'Опись ' + str(inventory_number)
            return (
                heading_str + ' ' + inventory_title + ' ' + heading_str
                + '\n\n' + self.inventories[inventory_number].get_page_text(
                    separate, heading_level + 1
                )
            )

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for fund."""
        heading_str = '=' * heading_level
        if separate:
            return (
                '{{Фонд|archive=' + self.parent.get_title_str()
                + '|fund=' + self.get_number_str()
                + '|fund_annotation=' + (self.annotation or '') + '}}\n\n'
                + heading_str + ' Описи ' + heading_str + '\n\n'
                + '\n'.join(list(map(
                    lambda inventory_number:
                    self.get_inventory_link_page_text(
                        inventory_number, separate, heading_level + 1
                    ),
                    sorted(self.inventories.keys(), key=lambda k: k or -1)
                )))
                + '\n'
            )
        else:
            return (
                '{{Фонд|archive=' + self.parent.get_title_str()
                + '|fund=' + self.get_number_str()
                + '|fund_annotation=' + (self.annotation or '') + '}}\n\n'
                + '\n'.join(list(map(
                    lambda inventory_number:
                    self.get_inventory_link_page_text(
                        inventory_number, separate, heading_level
                    ),
                    sorted(self.inventories.keys(), key=lambda k: k or -1)
                )))
                + '\n'
            )

    def fetch_all(self) -> None:
        """Load all inventories from files."""
        for inventory in self.inventories.values():
            inventory.fetch()


@dataclasses.dataclass
class FundLink:
    """Link to fund, it may be stored in file and fetched."""

    parent: 'Archive'
    number: Optional[int]
    loaded_fund: Optional[Fund]

    @property
    def base_directory_path(self) -> Optional[pathlib.Path]:
        """Base directory path for all hierarchy."""
        return self.parent.base_directory_path

    def fetch(self) -> Fund:
        """Return fund, load from file if necessary."""
        if self.loaded_fund is not None:
            return self.loaded_fund

        if self.base_directory_path is None:
            raise ValueError()
        fund_file_path = self.base_directory_path.joinpath(
            'archive' + self.parent.get_title_hash()
        ).joinpath(
            'fund' + self.get_number_str()
        ).joinpath(
            'list.json'
        )
        with open(fund_file_path, 'rt') as fund_file:
            data = json.load(fund_file)
        self.loaded_fund = Fund.from_json_dict(self.parent, data)

        return self.loaded_fund

    @property
    def fund(self) -> Fund:
        """Fund object."""
        return self.fetch()

    def get_number_str(self) -> str:
        """Return string representation of number."""
        return get_number_str(self.number)

    @property
    def annotation(self) -> Optional[str]:
        """Fund annotation."""
        return self.fund.annotation

    @annotation.setter
    def annotation(self, value: Optional[str]) -> None:
        """Fund annotation."""
        self.fund.annotation = None

    def append(self, item: FullItem) -> None:
        """Add item."""
        return self.fund.append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return self.fund.get_json_dict()

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for fund."""
        return self.fund.get_page_text(separate, heading_level)

    @property
    def inventories(self) -> Dict[Optional[int], InventoryLink]:
        """Fund items."""
        return self.fund.inventories


@dataclasses.dataclass
class Archive:
    """Archive data with funds."""

    parent: 'ArchiveList'
    title: Optional[str]
    funds: Dict[Optional[int], FundLink]

    @property
    def base_directory_path(self) -> Optional[pathlib.Path]:
        """Base directory path for all hierarchy."""
        return self.parent.base_directory_path

    def append(self, item: FullItem) -> None:
        """Add item."""
        if item.fund_number not in self.funds:
            self.funds[item.fund_number] = FundLink(
                self, item.fund_number,
                Fund(
                    self, item.fund_number, {}, item.fund_annotation
                )
            )
        fund = self.funds[item.fund_number]
        if item.fund_annotation:
            fund.annotation = item.fund_annotation
        fund.append(item)

    def get_title_str(self) -> str:
        """Return title string or empty string."""
        return self.title or ''

    def get_title_hash(self) -> str:
        """Get SHA3-256 hash of title as hex string."""
        return hashlib.sha3_256(
            self.get_title_str().encode('utf-8')
        ).hexdigest()

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'title': self.title,
            'funds':
            {get_number_str(fund_number): None for fund_number in self.funds}
        }

    @staticmethod
    def from_json_dict(
        parent: 'ArchiveList', data: Any
    ) -> 'Archive':
        """Create from data loaded from JSON."""
        result = Archive(parent, data['title'], {})
        for fund_number_str, fund in data['funds'].items():
            fund_number = get_str_number(fund_number_str)
            if fund is None:
                result.funds[fund_number] = FundLink(
                    result, fund_number, None
                )
            else:
                result.funds[fund_number] = FundLink(
                    result, fund_number,
                    Fund.from_json_dict(result, fund)
                )
        return result

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without funds."""
        return {
            'title': self.title
        }

    def get_fund_link_page_text(
        self, fund_number: Optional[int], separate: bool, heading_level: int
    ) -> str:
        """Return page wikitext for fund in archive."""
        if separate:
            return (
                '{{СсылкаНаФонд|archive=' + self.get_title_str()
                + '|fund=' + get_number_str(fund_number) + '}}'
            )
        else:
            heading_str = '=' * heading_level
            fund_title: str
            if fund_number is None:
                fund_title = 'Неизвестный фонд'
            else:
                fund_title = 'Фонд ' + str(fund_number)
            return (
                heading_str + ' ' + fund_title + ' ' + heading_str + '\n\n'
                + self.funds[fund_number].get_page_text(
                    separate, heading_level + 1
                )
            )

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for archive."""
        heading_str = '=' * heading_level
        return (
            '{{Архив|archive=' + self.get_title_str() + '}}\n\n'
            + heading_str + ' Фонды ' + heading_str + '\n\n'
            + '\n'.join(list(map(
                lambda fund_number:
                self.get_fund_link_page_text(
                    fund_number, separate, heading_level + 1
                ),
                sorted(self.funds.keys(), key=lambda k: k or -1)
            )))
            + '\n'
        )

    def fetch_all(self) -> None:
        """Load all funds and inventories from files."""
        for fund in self.funds.values():
            fund.fetch().fetch_all()


@dataclasses.dataclass
class ArchiveLink:
    """Link to archive, it may be stored in file and fetched."""

    parent: 'ArchiveList'
    title: Optional[str]
    loaded_archive: Optional[Archive]

    @property
    def base_directory_path(self) -> Optional[pathlib.Path]:
        """Base directory path for all hierarchy."""
        return self.parent.base_directory_path

    def fetch(self) -> Archive:
        """Return archive, load from file if necessary."""
        if self.loaded_archive is not None:
            return self.loaded_archive

        if self.base_directory_path is None:
            raise ValueError()
        archive_file_path = self.base_directory_path.joinpath(
            'archive' + self.get_title_hash()
        ).joinpath(
            'list.json'
        )
        with open(archive_file_path, 'rt') as archive_file:
            data = json.load(archive_file)
        self.loaded_archive = Archive.from_json_dict(self.parent, data)

        return self.loaded_archive

    @property
    def archive(self) -> Archive:
        """Archive object."""
        return self.fetch()

    def get_title_str(self) -> str:
        """Return title string or empty string."""
        return self.title or ''

    def get_title_hash(self) -> str:
        """Get SHA3-256 hash of title as hex string."""
        return hashlib.sha3_256(
            self.get_title_str().encode('utf-8')
        ).hexdigest()

    def append(self, item: FullItem) -> None:
        """Add item."""
        return self.archive.append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return self.archive.get_json_dict()

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for archive."""
        return self.archive.get_page_text(separate, heading_level)

    @property
    def funds(self) -> Dict[Optional[int], FundLink]:
        """Archive funds."""
        return self.archive.funds


@dataclasses.dataclass
class ArchiveList:
    """Archives data."""

    base_directory_path: Optional[pathlib.Path]
    archives: Dict[Optional[str], ArchiveLink]

    def append(self, item: FullItem) -> None:
        """Add item."""
        if item.archive_title not in self.archives:
            self.archives[item.archive_title] = ArchiveLink(
                self, item.archive_title,
                Archive(
                    self, item.archive_title, {}
                )
            )
        self.archives[item.archive_title].append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return list(self.archives)

    @staticmethod
    def from_json_dict(
        data: Any, base_directory_path: pathlib.Path
    ) -> 'ArchiveList':
        """Create from data loaded from JSON."""
        result = ArchiveList(base_directory_path, {})
        if isinstance(data, list):
            result.archives = {
                archive.title: archive
                for archive in map(
                    lambda archive_title_str:
                    ArchiveLink(result, archive_title_str or None, None),
                    data
                )
            }
        else:
            for archive_title_str, archive in data['archives'].items():
                archive_title: Optional[str]
                if archive_title_str:
                    archive_title = archive_title_str
                else:
                    archive_title = None
                if archive is None:
                    result.archives[archive_title] = ArchiveLink(
                        result, archive_title, None
                    )
                else:
                    result.archives[archive_title] = ArchiveLink(
                        result, archive_title,
                        Archive.from_json_dict(result, archive)
                    )
        return result

    def get_page_text(self, separate: bool, heading_level: int) -> str:
        """Return page wikitext for archive list."""
        heading_str = '=' * heading_level
        return (
            heading_str + ' Архивы ' + heading_str + '\n\n'
            + '\n'.join(list(map(
                lambda archive_title:
                f'* [[{archive_title}]]',
                sorted(
                    filter(bool, self.archives.keys()),
                    key=lambda k: k or -1
                )
            )))
            + '\n'
        )

    def fetch_all(self) -> None:
        """Load all archives, funds and inventories from files."""
        for archive in self.archives.values():
            archive.fetch().fetch_all()


def get_search_result_fields(
    data: List[Dict[str, Union[str, List[str]]]], url: Optional[str]
) -> FullItem:
    """Return tuple of archive name, fund number and inventory number."""
    archive_title: Optional[str] = None
    fund_number: Optional[int] = None
    inventory_number: Optional[int] = None
    item_number: Optional[str] = None

    fund_annotation: Optional[str] = None
    inventory_annotation: Optional[str] = None
    item_annotation: Optional[str] = None

    for field in data:
        content: Optional[str] = None
        if 'content' in field:
            content = ' '.join(
                list(map(
                    lambda s: strip_advanced(s).strip(),
                    field['content']
                ))
            )
        else:
            content = None
        if 'title' not in field:
            continue
        title = ''.join(field['title'])
        regex_result_fund = re.match(r'Фонд №(| )(\d+)', title)
        if regex_result_fund:
            fund_number = int(regex_result_fund.groups()[1])
            fund_annotation = process_annotation(content)
            continue
        regex_result_inventory = re.match(r'Опись №(| )(\d+)', title)
        if regex_result_inventory:
            inventory_number = int(regex_result_inventory.groups()[1])
            inventory_annotation = process_annotation(content)
            continue
        regex_result_item = re.match(r'Единица №(| )(\d+)', title)
        if regex_result_item:
            item_number = regex_result_item.groups()[1]
            item_annotation = process_annotation(content)
            continue
        if content is None:
            continue
        if field['title'] == 'Полное название архива':
            archive_title = process_archive_title(content)
            continue

    return FullItem(
        Item(
            None, item_number, item_annotation, data, None, None, url
        ),
        archive_title, fund_number, inventory_number,
        fund_annotation, inventory_annotation
    )


def write_archives_files(
    archives: ArchiveList, output_directory_path: pathlib.Path,
    iterator_wrapper: Callable[
        [Iterable[Tuple[Optional[str], ArchiveLink]]],
        ContextManager[Iterable[Tuple[Optional[str], ArchiveLink]]]
    ]
) -> None:
    """Write archives data to directory."""
    output_directory_path.mkdir(exist_ok=True)

    archive_list_file_path = output_directory_path.joinpath('list.json')
    with open(archive_list_file_path, 'wt') as archive_list_file:
        json.dump(
            archives.get_json_dict(), archive_list_file, ensure_ascii=False,
            indent=4, sort_keys=True
        )

    with iterator_wrapper(archives.archives.items()) as iterator:
        for _, archive in iterator:
            archive_title_hash = archive.get_title_hash()
            output_archive_directory_path = output_directory_path.joinpath(
                f'archive{archive_title_hash}'
            )
            output_archive_directory_path.mkdir(exist_ok=True)

            fund_list_file_path = output_archive_directory_path.joinpath(
                'list.json'
            )
            with open(fund_list_file_path, 'wt') as fund_list_file:
                json.dump(
                    archive.get_json_dict(), fund_list_file,
                    ensure_ascii=False, indent=4, sort_keys=True
                )

            for fund in archive.funds.values():
                fund_number_str = fund.get_number_str()
                output_inventory_directory_path = (
                    output_archive_directory_path.joinpath(
                        f'fund{fund_number_str}'
                    )
                )
                output_inventory_directory_path.mkdir(exist_ok=True)
                inventory_list_file_path = (
                    output_inventory_directory_path.joinpath(
                        'list.json'
                    )
                )
                with open(
                    inventory_list_file_path, 'wt'
                ) as inventory_list_file:
                    json.dump(
                        fund.get_json_dict(), inventory_list_file,
                        ensure_ascii=False, indent=4, sort_keys=True
                    )
                for inventory in fund.inventories.values():
                    inventory_number_str = inventory.get_number_str()
                    output_file_path = (
                        output_inventory_directory_path.joinpath(
                            f'inventory{inventory_number_str}.json'
                        )
                    )
                    with open(output_file_path, 'wt') as output_file:
                        json.dump(
                            inventory.get_json_dict(), output_file,
                            ensure_ascii=False, indent=4, sort_keys=True
                        )


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
def group_search_results(
    input_directory: str, output_directory: str
) -> None:
    """Group search results by archive name."""
    archives = ArchiveList(None, {})

    input_files_length = len(os.listdir(input_directory))
    input_files = pathlib.Path(input_directory).iterdir()
    with click.progressbar(
        input_files, show_pos=True, length=input_files_length
    ) as progress_bar1:
        for input_file_path in progress_bar1:
            if input_file_path.name == 'list.json':
                continue
            regex_result = re.match(
                r'([\d-]+)_(\d+)\.json', input_file_path.name
            )
            if regex_result is None:
                continue
            data_id = regex_result.group(1)
            data_kind = regex_result.group(2)
            url = SEARCH_DETAILS_URL + '?' + urllib.parse.urlencode(
                (('ID', data_id), ('Kind', data_kind))
            )
            with open(input_file_path, 'rt') as input_file:
                data = json.load(
                    input_file
                )
                item = (
                    get_search_result_fields(data, url)
                )

                archives.append(item)

    output_directory_path = pathlib.Path(output_directory)
    write_archives_files(
        archives, output_directory_path,
        lambda it: click.progressbar(it, show_pos=True)
    )


@click.command()
@click.option(
    '--separate/--no-separate',
    default=False,
    help='Link to funds instead of embedding them'
)
@click.argument(
    'input-file',
    type=click.File(mode='rt')
)
@click.argument(
    'output-file',
    type=click.File(mode='wt')
)
def generate_archive_page(
    separate: bool, input_file: TextIO, output_file: TextIO
) -> None:
    """Generate wiki-page for archive and write it to file."""
    archive_list = ArchiveList(pathlib.Path(input_file.name).parent.parent, {})
    archive = Archive.from_json_dict(archive_list, json.load(input_file))
    output_file.write(archive.get_page_text(separate, 2))


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'archive-name',
    type=click.STRING
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
def generate_archive_pages(
    input_directory: str, archive_name: str, output_directory: str
) -> None:
    """Generate wiki-text pages for archive."""
    input_directory_path = pathlib.Path(input_directory)
    input_archive_file_path = input_directory_path.joinpath('list.json')
    with open(input_archive_file_path, 'rt') as input_archive_file:
        archives = ArchiveList.from_json_dict(
            json.load(input_archive_file),
            input_directory_path
        )
    if archive_name not in archives.archives:
        raise click.ClickException(f'Archive {archive_name} not found')
    archive = archives.archives[archive_name]

    output_directory_path = pathlib.Path(output_directory)
    output_directory_path.mkdir(exist_ok=True)

    output_archive_file_path = output_directory_path.joinpath(
        (archive.get_title_str() or 'Неизвестный архив') + '.txt'
    )
    with open(output_archive_file_path, 'wt') as output_archive_file:
        output_archive_file.write(archive.get_page_text(True, 2))

    output_archive_directory_path = output_directory_path.joinpath(
        archive.get_title_str() or 'Неизвестный архив'
    )
    output_archive_directory_path.mkdir(exist_ok=True)

    with click.progressbar(
        archive.funds.values(), show_pos=True
    ) as progress_bar:
        for fund in progress_bar:
            page_title: str
            if fund.number is None:
                page_title = 'Неизвестный фонд'
            else:
                page_title = 'Фонд ' + str(fund.number)
            output_fund_file_path = output_archive_directory_path.joinpath(
                page_title + '.txt'
            )
            with open(output_fund_file_path, 'wt') as output_fund_file:
                output_fund_file.write(fund.get_page_text(False, 2))


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
def generate_archives_pages(
    input_directory: str, output_directory: str
) -> None:
    """Generate wiki-text pages for all archives."""
    input_directory_path = pathlib.Path(input_directory)
    input_archive_file_path = input_directory_path.joinpath('list.json')
    with open(input_archive_file_path, 'rt') as input_archive_file:
        archives = ArchiveList.from_json_dict(
            json.load(input_archive_file),
            input_directory_path
        )

    output_directory_path = pathlib.Path(output_directory)
    output_directory_path.mkdir(exist_ok=True)

    output_archive_list_file_path = output_directory_path.joinpath(
        'Список архивов.txt'
    )
    with open(output_archive_list_file_path, 'wt') as output_archive_file:
        output_archive_file.write(archives.get_page_text(True, 2))

    with click.progressbar(
        list(archives.archives.values()), show_pos=True
    ) as progress_bar:
        for archive in progress_bar:
            archive_title_str = archive.get_title_str() or 'Неизвестный архив'
            output_archive_file_path = output_directory_path.joinpath(
                archive_title_str + '.txt'
            )
            with open(output_archive_file_path, 'wt') as output_archive_file:
                output_archive_file.write(archive.get_page_text(True, 2))

            output_archive_directory_path = output_directory_path.joinpath(
                archive_title_str
            )
            output_archive_directory_path.mkdir(exist_ok=True)

            for fund in archive.funds.values():
                fund_title_str: str
                if fund.number is None:
                    fund_title_str = 'Неизвестный фонд'
                else:
                    fund_title_str = 'Фонд ' + str(fund.number)
                output_fund_file_path = output_archive_directory_path.joinpath(
                    fund_title_str + '.txt'
                )
                with open(output_fund_file_path, 'wt') as output_fund_file:
                    output_fund_file.write(fund.get_page_text(True, 2))

                output_fund_directory_path = (
                    output_archive_directory_path.joinpath(
                        fund_title_str
                    )
                )
                output_fund_directory_path.mkdir(exist_ok=True)

                for inventory in fund.inventories.values():
                    inventory_title_str: str
                    if inventory.number is None:
                        inventory_title_str = 'Неизвестная опись'
                    else:
                        inventory_title_str = 'Опись ' + str(inventory.number)
                    output_inventory_file_path = (
                        output_fund_directory_path.joinpath(
                            inventory_title_str + '.txt'
                        )
                    )
                    with open(
                        output_inventory_file_path, 'wt'
                    ) as output_inventory_file:
                        output_inventory_file.write(
                            inventory.get_page_text(True, 2)
                        )

                    output_inventory_directory_path = (
                        output_fund_directory_path.joinpath(
                            inventory_title_str
                        )
                    )
                    output_inventory_directory_path.mkdir(exist_ok=True)

                    for item in inventory.items.values():
                        item_title_str: str
                        if item.item_number is None:
                            item_title_str = 'Неизвестно'
                        else:
                            item_title_str = item.get_number_str()
                        output_item_file_path = (
                            output_inventory_directory_path.joinpath(
                                item_title_str + '.txt'
                            )
                        )
                        with open(
                            output_item_file_path, 'wt'
                        ) as output_item_file:
                            output_item_file.write(
                                item.get_page_text()
                            )


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'rename-file',
    type=click.File(mode='rt')
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
def rename_archives(
    input_directory: str, rename_file: TextIO, output_directory: str
) -> None:
    """Rename archives according to JSON dictionary."""
    input_directory_path = pathlib.Path(input_directory)
    input_archive_file_path = input_directory_path.joinpath('list.json')
    with open(input_archive_file_path, 'rt') as input_archive_file:
        archives = ArchiveList.from_json_dict(
            json.load(input_archive_file),
            input_directory_path
        )

    archives.fetch_all()

    rename_dict: Dict[str, str] = json.load(rename_file)
    for old_name, new_name in rename_dict.items():
        if old_name == new_name:
            continue
        if old_name not in archives.archives:
            continue
        if new_name in archives.archives:
            raise click.ClickException(
                f'Archive with name {new_name} already exists'
            )
        archive = archives.archives.pop(old_name)
        archive.title = new_name
        archive.archive.title = new_name
        archives.archives[new_name] = archive

    output_directory_path = pathlib.Path(output_directory)
    write_archives_files(
        archives, output_directory_path,
        lambda it: click.progressbar(it, show_pos=True)
    )


@click.command()
@click.argument(
    'input-file',
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True)
)
@click.argument(
    'output-directory',
    type=click.Path(file_okay=False, dir_okay=True, writable=True)
)
@click.argument(
    'archive-name', type=click.STRING
)
def load_spreadsheet_results(
    input_file: str, output_directory: str, archive_name: str
) -> None:
    """Load data from spreadsheet file and write to directory."""
    input_spreadhseet = pyexcel.load(input_file)

    input_spreadhseet_iterator = iter(input_spreadhseet)
    next(input_spreadhseet_iterator)
    next(input_spreadhseet_iterator)

    archives = ArchiveList(None, {})

    for line in input_spreadhseet_iterator:
        item_number: Optional[str]
        if not line[5]:
            item_number = None
        if isinstance(line[5], float):
            item_number = str(int(line[5]))
        else:
            item_number = str(line[5])
        item = FullItem(
            Item(
                None, item_number, str(line[0]), [], get_str_number(line[1]),
                get_str_number(line[2]), str(line[6])
            ),
            archive_name, get_str_number(line[4]), get_str_number(line[3]),
            str(line[7]), None
        )
        archives.append(item)

    output_directory_path = pathlib.Path(output_directory)
    write_archives_files(
        archives, output_directory_path,
        lambda it: click.progressbar(it, show_pos=True)
    )


cli.add_command(list_organizations)
cli.add_command(fetch_organization_data)
cli.add_command(list_search_results)
cli.add_command(fetch_search_results)
cli.add_command(group_search_results)
cli.add_command(generate_archive_page)
cli.add_command(generate_archive_pages)
cli.add_command(generate_archives_pages)
cli.add_command(rename_archives)
cli.add_command(load_spreadsheet_results)

if __name__ == '__main__':
    cli()
