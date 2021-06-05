"""Script to fetch data from rusarchives.ru."""
import click

from cfc_rusarchives import (fetch_search_results, generate_archive_pages,
                             generate_archives_pages, group_search_results,
                             list_search_results, load_spreadsheet_results,
                             rename_archives)
from rusarchives import fetch_organization_data, list_organizations
from temples import fetch_temples_data, generate_temples_pages


@click.group()
def cli() -> None:
    """Run command line."""
    pass


cli.add_command(list_organizations)
cli.add_command(fetch_organization_data)

cli.add_command(list_search_results)
cli.add_command(fetch_search_results)
cli.add_command(group_search_results)
cli.add_command(generate_archive_pages)
cli.add_command(generate_archives_pages)
cli.add_command(rename_archives)
cli.add_command(load_spreadsheet_results)

cli.add_command(fetch_temples_data)
cli.add_command(generate_temples_pages)

if __name__ == '__main__':
    cli()
