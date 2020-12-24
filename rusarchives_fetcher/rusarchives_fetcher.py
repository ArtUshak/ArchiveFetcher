"""Script to fetch data from rusarchives.ru."""
import copy
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

    # TODO: result count
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


def get_search_result_fields(
    data: List[Dict[str, Union[str, List[str]]]]
) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """Return tuple of archive name, fund number and inventory number."""
    archive_title: Optional[str] = None
    fund_number: Optional[int] = None
    inventory_number: Optional[int] = None
    for field in data:
        if 'title' not in field:
            continue
        title = ''.join(field['title'])
        regex_result_fund = re.match(r'Фонд №(| )(\d+)', title)
        if regex_result_fund:
            fund_number = int(regex_result_fund.groups()[1])
            continue
        regex_result_inventory = re.match(r'Опись №(| )(\d+)', title)
        if regex_result_inventory:
            inventory_number = int(regex_result_inventory.groups()[1])
            continue
        if 'content' not in field:
            continue
        content = ' '.join(
            list(map(
                lambda s: strip_advanced(s).strip(),
                field['content']
            ))
        )
        if field['title'] == 'Полное название архива':
            archive_title = process_archive_title(content)
            continue
    return archive_title, fund_number, inventory_number


def append_item_data(
    archives: Dict[str, Dict[str, Dict[str, Any]]],
    archive_title_str: str, fund_number_str: str, inventory_number_str: str,
    data: Any,
) -> None:
    if archive_title_str not in archives:
        archives[archive_title_str] = {}
    archive_data = archives[archive_title_str]
    if fund_number_str not in archive_data:
        archive_data[fund_number_str] = {}
    fund_data = archive_data[fund_number_str]
    if inventory_number_str not in fund_data:
        fund_data[inventory_number_str] = []
    inventory_data = fund_data[inventory_number_str]
    inventory_data.append(data)


@click.command()
@click.option(
    '--group-by', type=click.Choice(['fund', 'inventory']), default=None
)
@click.argument(
    'input-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument(
    'output-directory',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True)
)
def group_search_results(
    group_by: Optional[str], input_directory: str, output_directory: str
) -> None:
    """Group search results by archive name."""
    archives: Dict[str, Dict[str, Dict[str, Any]]] = {}

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
                if group_by is not None:
                    for archive_title_str, archive_data in data.items():
                        for fund_number_str, fund_data in archive_data.items():
                            for inventory_number_str, inventory_data in (
                                fund_data.items()
                            ):
                                for data in inventory_data:
                                    append_item_data(
                                        archives, archive_title_str,
                                        fund_number_str, inventory_number_str,
                                        data
                                    )
                else:
                    archive_title, fund_number, inventory_number = (
                        get_search_result_fields(data)
                    )

                    archive_title_str = ''
                    if (archive_title is not None):
                        archive_title_str = archive_title
                    fund_number_str = ''
                    if (fund_number_str is not None):
                        fund_number_str = str(fund_number)
                    inventory_number_str = ''
                    if (inventory_number_str is not None):
                        inventory_number_str = str(inventory_number)

                    append_item_data(
                        archives, archive_title_str, fund_number_str,
                        inventory_number_str, data
                    )

    output_directory_path = pathlib.Path(output_directory)
    archive_list_file_path = output_directory_path.joinpath('list.json')
    archive_list: Dict[str, str] = {
        archive_title_str:
        hashlib.sha3_256(archive_title_str.encode('utf8')).hexdigest()
        for archive_title_str in archives.keys()
    }
    with open(archive_list_file_path, 'wt') as archive_list_file:
        json.dump(
            archive_list, archive_list_file, ensure_ascii=False,
            indent=4
        )

    with click.progressbar(archives.items(), show_pos=True) as progress_bar2:
        for archive_title_str, archive_data in progress_bar2:
            archive_title_hash = hashlib.sha3_256(
                (archive_title_str.encode('utf8'))
            ).hexdigest()
            if group_by is not None:
                output_archive_directory_path = output_directory_path.joinpath(
                    f'archive{archive_title_hash}'
                )
                output_archive_directory_path.mkdir(exist_ok=True)

                fund_list_file_path = output_archive_directory_path.joinpath(
                    'list.json'
                )
                with open(fund_list_file_path, 'wt') as fund_list_file:
                    json.dump(
                        list(archive_data.keys()), fund_list_file,
                        ensure_ascii=False, indent=4
                    )

                for fund_number_str, fund_data in archive_data.items():
                    if group_by == 'inventory':
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
                                list(fund_data.keys()),
                                inventory_list_file,
                                ensure_ascii=False, indent=4
                            )
                        for (
                            inventory_number_str, inventory_data
                        ) in fund_data.items():
                            inventory_data_named = {
                                archive_title_str:
                                {
                                    fund_number_str: {
                                        inventory_number_str: inventory_data
                                    }
                                }
                            }
                            output_file_path = (
                                output_inventory_directory_path.joinpath(
                                    f'inventory{inventory_number_str}.json'
                                )
                            )
                            with open(output_file_path, 'wt') as output_file:
                                json.dump(
                                    inventory_data_named, output_file,
                                    ensure_ascii=False, indent=4
                                )
                    else:
                        fund_data_named = {
                            archive_title_str:
                            {
                                fund_number_str: fund_data
                            }
                        }
                        output_file_path = (
                            output_archive_directory_path.joinpath(
                                f'fund{fund_number_str}.json'
                            )
                        )
                        with open(output_file_path, 'wt') as output_file:
                            json.dump(
                                fund_data_named, output_file,
                                ensure_ascii=False, indent=4
                            )
            else:
                output_file_path = output_directory_path.joinpath(
                    f'archive{archive_title_hash}.json'
                )
                archive_data_named = {
                    archive_title_str: archive_data
                }
                with open(output_file_path, 'wt') as output_file:
                    json.dump(
                        archive_data_named, output_file, ensure_ascii=False,
                        indent=4
                    )


cli.add_command(list_organizations)
cli.add_command(fetch_organization_data)
cli.add_command(list_search_results)
cli.add_command(fetch_search_result)
cli.add_command(group_search_results)

if __name__ == '__main__':
    cli()
