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
from typing import (Any, Dict, Iterator, List, Optional, TextIO, Tuple, Union,
                    cast)

import click
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
    if x is None:
        return ''
    return str(x)


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
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True)
)
def fetch_search_result(
    first_item: Optional[int], item_count: Optional[int],
    input_file: TextIO, output_directory: str
) -> None:
    """Get data about search result and write it to JSON file."""
    data = json.load(input_file)
    if first_item is not None:
        data = data[first_item:]
    if item_count is not None:
        data = data[:item_count]

    session = requests_html.HTMLSession()
    output_directory_path = pathlib.Path(output_directory)

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
    title7 = re.sub(
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
        title6,
        flags=re.IGNORECASE
    )
    return title7.replace('"', '').strip()


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
    archive_title: Optional[str]
    fund_number: Optional[int]
    inventory_number: Optional[int]
    item_number: Optional[int]
    fund_annotation: Optional[str]
    inventory_annotation: Optional[str]
    item_annotation: Optional[str]
    data: ItemData

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'archive_title': self.archive_title,
            'fund_number': self.fund_number,
            'inventory_number': self.inventory_number,
            'item_number': self.item_number,
            'fund_annotation': self.fund_annotation,
            'inventory_annotation': self.inventory_annotation,
            'item_annotation': self.item_annotation,
            'data': self.data
        }

    @staticmethod
    def from_json_dict(parent: 'Inventory', data: Any) -> 'Item':
        return Item(
            parent, data['archive_title'], data['fund_number'],
            data['inventory_number'], data['item_number'],
            data['fund_annotation'], data['inventory_annotation'],
            data['item_annotation'], data['data']
        )

    def get_number_str(self) -> str:
        """Return string representation of number."""
        if self.item_number is None:
            return ''
        else:
            return str(self.item_number)

    def get_page_text(self) -> str:
        """Return page wikitext for item."""
        return self.item_annotation or ''


@dataclasses.dataclass
class Inventory:
    """Inventory data with items."""

    parent: 'Fund'
    number: Optional[int]
    items: Dict[Optional[int], Item]
    annotation: Optional[str]

    def append(self, item: Item) -> None:
        """Add item."""
        self.items[item.item_number] = item

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

    def get_full_json_dict(self) -> Any:
        """Get dictionary representation of inventory and parents for JSON."""
        fund_data = self.parent.get_partial_json_dict()
        fund_data['inventories'] = {
            self.get_number_str(): self.get_json_dict()
        }
        archive_data = self.parent.parent.get_partial_json_dict()
        archive_data['funds'] = {
            self.parent.get_number_str(): fund_data
        }
        return {
            self.parent.parent.get_title_str(): archive_data
        }

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without items."""
        return {
            'number': self.number,
            'annotation': self.annotation
        }

    @staticmethod
    def from_json_dict(parent: 'Fund', data: Any) -> 'Inventory':
        result = Inventory(
            parent, data['number'], {}, data['annotation']
        )
        for item_number_str, item in data['items'].items():
            result.items[item_number_str] = Item.from_json_dict(result, item)
        return result

    def get_number_str(self) -> str:
        """Return string representation of number."""
        if self.number is None:
            return ''
        else:
            return str(self.number)

    def get_page_text(self) -> str:
        """Return page wikitext for item."""
        return (
            '{{Опись|archive=' + (self.parent.parent.title or '') + '|fund='
            + self.parent.get_number_str() + '|inventory='
            + self.get_number_str() + '}}\n\n'
            + '== Единицы хранения ==\n\n'
            + '\n\n'.join(list(map(
                lambda item:
                '=== Единица хранения ' + item.get_number_str() + ' ===\n\n'
                + (item.get_page_text() or ''),
                self.items.values()
            ))) + '\n'
        )


@dataclasses.dataclass
class InventoryLink:
    """Optional Inventory."""

    inventory: Optional[Inventory]

    def is_ok(self) -> bool:
        return self.inventory is not None

    def get_number_str(self) -> str:
        if self.inventory is None:
            raise ValueError()
        return self.inventory.get_number_str()

    def get_full_json_dict(self) -> Any:
        if self.inventory is None:
            return None
        return self.inventory.get_full_json_dict()

    def append(self, item: Item) -> None:
        if self.inventory is None:
            raise ValueError()
        return self.inventory.append(item)


@dataclasses.dataclass
class Fund:
    """Fund data with inventories."""

    parent: 'Archive'
    number: Optional[int]
    inventories: Dict[Optional[int], InventoryLink]
    annotation: Optional[str]

    def append(self, item: Item) -> None:
        """Add item."""
        if item.inventory_number not in self.inventories:
            self.inventories[item.inventory_number] = InventoryLink(Inventory(
                self, item.inventory_number, {}, item.inventory_annotation
            ))
        inventory = self.inventories[item.inventory_number]
        if inventory is not None:
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

    def get_full_json_dict(self) -> Any:
        """Get dictionary representation of inventory and parents for JSON."""
        archive_data = self.parent.get_partial_json_dict()
        archive_data['funds'] = {
            self.get_number_str(): self.get_json_dict()
        }
        return {
            self.parent.get_title_str(): archive_data
        }

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without inventories."""
        return {
            'number': self.number,
            'annotation': self.annotation
        }

    @staticmethod
    def from_json_dict(parent: 'Archive', data: Any) -> 'Fund':
        result = Fund(
            parent, data['number'], {}, data['annotation']
        )
        for inventory_number_str, inventory in data['inventories'].items():
            if inventory is None:
                result.inventories[inventory_number_str] = InventoryLink(None)
            else:
                result.inventories[inventory_number_str] = InventoryLink(
                    Inventory.from_json_dict(result, inventory)
                )
        return result

    def get_number_str(self) -> str:
        """Return string representation of number."""
        if self.number is None:
            return ''
        else:
            return str(self.number)

    def get_page_text(self) -> str:
        """Return page wikitext for fund."""
        return (
            '{{Фонд|archive=' + (self.parent.title or '') + '|fund='
            + self.get_number_str() + '}}\n\n'
            + '== Описи ==\n\n'
            + '\n'.join(list(map(
                lambda inventory_number:
                '* {{СсылкаНаОпись|archive=' + (self.parent.title or '')
                + '|fund=' + self.get_number_str() + '|inventory='
                + get_number_str(inventory_number) + '}}',
                self.inventories.keys()
            )))
            + '\n'
        )


@dataclasses.dataclass
class FundLink:
    """Optional Fund."""

    fund: Optional[Fund]

    def is_ok(self) -> bool:
        return self.fund is not None

    def get_number_str(self) -> str:
        if self.fund is None:
            raise ValueError()
        return self.fund.get_number_str()

    def append(self, item: Item) -> None:
        if self.fund is None:
            raise ValueError()
        return self.fund.append(item)

    def get_full_json_dict(self) -> Any:
        if self.fund is None:
            return None
        return self.fund.get_full_json_dict()

    @property
    def inventories(self) -> Dict[Optional[int], InventoryLink]:
        if self.fund is None:
            raise ValueError()
        return self.fund.inventories


@dataclasses.dataclass
class Archive:
    """Archive data with funds."""

    parent: 'ArchiveList'
    title: Optional[str]
    funds: Dict[Optional[int], FundLink]

    def append(self, item: Item) -> None:
        """Add item."""
        if item.fund_number not in self.funds:
            self.funds[item.fund_number] = FundLink(Fund(
                self, item.fund_number, {}, item.fund_annotation
            ))
        self.funds[item.fund_number].append(item)

    def get_title_str(self) -> str:
        """Return title string or empty string."""
        return self.title or ''

    def get_title_hash(self) -> str:
        """Get SHA3-256 hash of title as hex string."""
        return hashlib.sha3_256(
            self.get_title_str().encode('utf8')
        ).hexdigest()

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return {
            'title': self.title,
            'funds':
            {get_number_str(fund_number): None for fund_number in self.funds}
        }

    def get_full_json_dict(self) -> Any:
        """Get dictionary representation of inventory and parents for JSON."""
        return {
            self.get_title_str(): self.get_json_dict()
        }

    @staticmethod
    def from_json_dict(parent: 'ArchiveList', data: Any) -> 'Archive':
        result = Archive(parent, data['title'], {})
        for fund_number_str, fund in data['funds'].items():
            if fund is None:
                result.funds[fund_number_str] = FundLink(None)
            else:
                result.funds[fund_number_str] = FundLink(
                    Fund.from_json_dict(result, fund)
                )
        return result

    def get_partial_json_dict(self) -> Any:
        """Get dictionary representation for JSON without funds."""
        return {
            'title': self.title
        }

    def get_page_text(self) -> str:
        """Return page wikitext for archive."""
        return (
            '{{Архив|archive=' + (self.title or '') + '}}\n\n'
            + '== Фонды ==\n\n'
            + '\n'.join(list(map(
                lambda fund_number:
                '* {{СсылкаНаФонд|archive=' + self.get_title_str()
                + '|fund=' + get_number_str(fund_number) + '}}',
                self.funds.keys()
            )))
            + '\n'
        )


@dataclasses.dataclass
class ArchiveList:
    """Archives data."""

    archives: Dict[Optional[str], Archive]

    def append(self, item: Item) -> None:
        """Add item."""
        if item.archive_title not in self.archives:
            self.archives[item.archive_title] = Archive(
                self, item.archive_title, {}
            )
        self.archives[item.archive_title].append(item)

    def get_json_dict(self) -> Any:
        """Get dictionary representation for JSON."""
        return list(self.archives)

    @staticmethod
    def from_json_dict(data: Any) -> 'ArchiveList':
        result = ArchiveList({})
        result.archives = {
            archive_title_str: Archive.from_json_dict(result, archive)
            for archive_title_str, archive in data.items()
        }
        return result


def get_search_result_fields(
    data: List[Dict[str, Union[str, List[str]]]]
) -> Item:
    """Return tuple of archive name, fund number and inventory number."""
    archive_title: Optional[str] = None
    fund_number: Optional[int] = None
    inventory_number: Optional[int] = None
    item_number: Optional[int] = None

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
            item_number = int(regex_result_item.groups()[1])
            item_annotation = process_annotation(content)
            continue
        if content is None:
            continue
        if field['title'] == 'Полное название архива':
            archive_title = process_archive_title(content)
            continue

    return Item(
        None, archive_title, fund_number, inventory_number, item_number,
        fund_annotation, inventory_annotation, item_annotation, data
    )


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'output-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True)
)
def group_search_results(
    input_directory: str, output_directory: str
) -> None:
    """Group search results by archive name."""
    archives = ArchiveList({})

    input_files_length = len(os.listdir(input_directory))
    input_files = pathlib.Path(input_directory).iterdir()
    with click.progressbar(
        input_files, show_pos=True, length=input_files_length
    ) as progress_bar1:
        for input_file_path in progress_bar1:
            if input_file_path.name == 'list.json':
                continue
            with open(input_file_path, 'rt') as input_file:
                data = json.load(
                    input_file
                )
                item = (
                    get_search_result_fields(data)
                )

                archives.append(item)

    output_directory_path = pathlib.Path(output_directory)

    archive_list_file_path = output_directory_path.joinpath('list.json')
    with open(archive_list_file_path, 'wt') as archive_list_file:
        json.dump(
            archives.get_json_dict(), archive_list_file, ensure_ascii=False,
            indent=4, sort_keys=True
        )

    with click.progressbar(
        archives.archives.items(), show_pos=True
    ) as progress_bar2:
        for _, archive in progress_bar2:
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
                    archive.get_full_json_dict(), fund_list_file,
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
                        fund.get_full_json_dict(), inventory_list_file,
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
                            inventory.get_full_json_dict(), output_file,
                            ensure_ascii=False, indent=4, sort_keys=True
                        )


@click.command()
@click.argument(
    'input-file',
    type=click.File(mode='rt')
)
@click.argument(
    'output-file',
    type=click.File(mode='wt')
)
def generate_inventory_page(
    input_file: TextIO, output_file: TextIO
) -> None:
    archives = ArchiveList.from_json_dict(json.load(input_file))
    inventory = list(
        list(
            list(archives.archives.values())[0].funds.values()
        )[0].inventories.values()
    )[0].inventory
    if inventory is None:
        raise ValueError()
    output_file.write(inventory.get_page_text())


@click.command()
@click.argument(
    'input-file',
    type=click.File(mode='rt')
)
@click.argument(
    'output-file',
    type=click.File(mode='wt')
)
def generate_fund_page(
    input_file: TextIO, output_file: TextIO
) -> None:
    archives = ArchiveList.from_json_dict(json.load(input_file))
    fund = list(
        list(archives.archives.values())[0].funds.values()
    )[0].fund
    if fund is None:
        raise ValueError()
    output_file.write(fund.get_page_text())


@click.command()
@click.argument(
    'input-file',
    type=click.File(mode='rt')
)
@click.argument(
    'output-file',
    type=click.File(mode='wt')
)
def generate_archive_page(
    input_file: TextIO, output_file: TextIO
) -> None:
    archives = ArchiveList.from_json_dict(json.load(input_file))
    archive = list(archives.archives.values())[0]
    output_file.write(archive.get_page_text())


@click.command()
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'output-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True)
)
def generate_archive_pages(
    input_directory: str, output_directory: str
) -> None:
    """Generate wiki-text pages for archive."""
    input_directory_path = pathlib.Path(input_directory)
    input_archive_file_path = input_directory_path.joinpath('list.json')
    with open(input_archive_file_path, 'rt') as input_archive_file:
        archives = ArchiveList.from_json_dict(
            json.load(input_archive_file)
        )
    archive = list(archives.archives.values())[0]

    output_directory_path = pathlib.Path(output_directory)

    output_archive_file_path = output_directory_path.joinpath(
        (archive.get_title_str() or 'Неизвестный архив') + '.txt'
    )
    with open(output_archive_file_path, 'wt') as output_archive_file:
        output_archive_file.write(archive.get_page_text())

    output_archive_directory_path = output_directory_path.joinpath(
        archive.get_title_str() or 'Неизвестный архив'
    )
    output_archive_directory_path.mkdir()

    input_files_length = len(os.listdir(input_directory))
    with click.progressbar(
        input_directory_path.iterdir(), show_pos=True,
        length=input_files_length
    ) as progress_bar:
        for input_fund_directory_path in progress_bar:
            if input_fund_directory_path.name == 'list.json':
                continue

            input_fund_file_path = input_fund_directory_path.joinpath(
                'list.json'
            )
            with open(input_fund_file_path, 'rt') as input_fund_file:
                archives = ArchiveList.from_json_dict(
                    json.load(input_fund_file)
                )
            fund = list(
                list(archives.archives.values())[0].funds.values()
            )[0].fund
            if fund is None:
                raise ValueError()
            page_title: str
            if fund.number is None:
                page_title = 'Неизвестный фонд'
            else:
                page_title = 'Фонд ' + str(fund.number)
            output_fund_file_path = output_archive_directory_path.joinpath(
                page_title + '.txt'
            )
            with open(output_fund_file_path, 'wt') as output_fund_file:
                output_fund_file.write(fund.get_page_text())

            output_fund_directory_path = (
                output_archive_directory_path.joinpath(
                    page_title
                )
            )
            output_fund_directory_path.mkdir()

            for input_inventory_file_path in (
                input_fund_directory_path.iterdir()
            ):
                if input_inventory_file_path.name == 'list.json':
                    continue

                with (
                    open(input_inventory_file_path, 'rt')
                ) as input_inventory_file:
                    archives = ArchiveList.from_json_dict(
                        json.load(input_inventory_file)
                    )
                inventory = list(list(
                    list(archives.archives.values())[0].funds.values()
                )[0].inventories.values())[0].inventory
                if inventory is None:
                    raise ValueError()
                if inventory.number is None:
                    page_title = 'Неизвестная опись'
                else:
                    page_title = 'Опись ' + str(inventory.number)
                output_inventory_file_path = (
                    output_fund_directory_path.joinpath(
                        page_title + '.txt'
                    )
                )
                with (
                    open(output_inventory_file_path, 'wt')
                ) as output_inventory_file:
                    output_inventory_file.write(inventory.get_page_text())


cli.add_command(list_organizations)
cli.add_command(fetch_organization_data)
cli.add_command(list_search_results)
cli.add_command(fetch_search_result)
cli.add_command(group_search_results)
cli.add_command(generate_inventory_page)
cli.add_command(generate_fund_page)
cli.add_command(generate_archive_page)
cli.add_command(generate_archive_pages)

if __name__ == '__main__':
    cli()
