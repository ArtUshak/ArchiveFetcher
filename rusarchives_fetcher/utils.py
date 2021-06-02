"""Common functions."""
import re
import time
from typing import Any, Dict, Iterator, Optional, Tuple

import aiohttp
import requests
import requests_html
from lxml.etree import Element

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


def get_str_str(s: str) -> Optional[str]:
    """Get string or `None` on empty or special string."""
    if not s:
        return None
    if s == 'null':
        return None
    if s == '#VALUE!':
        return None
    return s


def get_any_str(s: Any) -> Optional[str]:
    """Get string or `None`."""
    if not s:
        return None
    return str(s)


def get_str_number(s: Optional[str]) -> Optional[int]:
    """Get number from string or `None` on non-numeric string."""
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def get_number_keys(s: Optional[str]) -> Tuple[int, str, int]:
    """Get number from string or `None` on empty string."""
    if not s:
        return -1, '', -1
    regex_result = re.match(r'(\d*)([^\d]*)(\d*)', s)
    if not regex_result:
        return 0, s, -1
    part0 = regex_result.group(1)
    part1 = regex_result.group(2)
    part2 = regex_result.group(3)
    if part0:
        if part2:
            return int(part0), part1, int(part2)
        else:
            return int(part0), part1, -1
    else:
        return 0, s, 0


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


def lxml_iter_element_text_objects(element: Element) -> Iterator[str]:
    """
    Iterate over element texts as non-empty strings.
    """
    if element.text:
        text_str = strip_advanced(element.text.strip())
        if text_str:
            yield text_str

    for child in element:
        for child_str in lxml_iter_element_text_objects(child):
            yield child_str
        if child.tail:
            tail_str = strip_advanced(child.tail.strip())
            if tail_str:
                yield tail_str


def lxml_get_link_data(
    link_element: Element
) -> Optional[Tuple[str, str]]:
    """Return tuple of hyperlink URL and text if element is hyperlink."""
    try:
        href = link_element.attrib['href'].strip()
        if (
            href and not (href.startswith('#'))
            and not href.startswith(('javascript:', 'mailto:'))
        ):
            return href, link_element.text_content()
        else:
            return None
    except KeyError:
        return None


async def aiohttp_get(
    session: aiohttp.ClientSession, url: str,
    params: Optional[Dict[str, Any]] = None
) -> aiohttp.ClientResponse:
    """Perform GET request and return HTTP response. Retry on error."""
    for _ in range(MAX_TRY_NUM):
        try:
            response = await session.get(
                url, params=params
            )
            time.sleep(SLEEP_TIME_DEFAULT)
            return response
        except aiohttp.ClientConnectionError:
            time.sleep(SLEEP_TIME_DISCONNECTED)
    raise ValueError('Max request try num exceeded')  # TODO