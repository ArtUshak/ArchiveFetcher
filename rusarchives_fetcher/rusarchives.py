"""Functions to fetch from rusarchives.ru."""
import json
import sys
from typing import Dict, Iterator, List, Optional, TextIO, Tuple, Union, cast

import click
import requests_html

from utils import get_link_data, request_get

ROOT_URL = 'http://rusarchives.ru'
ARCHIVE_LIST_LOCAL_URL = '/state/list'


DictList = List[Dict[str, str]]
DictList1 = List[Dict[str, Union[str, DictList]]]
DictList2 = List[Dict[str, Union[str, DictList1]]]


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
