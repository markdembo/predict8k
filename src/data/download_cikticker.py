import requests
import re


def download(url):
    """Download csv from the web

    Args:
        url (string): URL to .csv of cik ticker matching
    """
    return requests.get(url).text


def repair_file(content):
    """Repair .csv file

    Args:
        content (string): content of .csv file
    """
    pattern = "(,(\d*),(.+),(.*),(.*,.*,))"

    for match in re.findall(pattern, content):
        correction = (
            ",{},{}{},{}"
            .format(match[1], match[2], match[3], match[4])
        )
        content = content.replace(match[0], correction)

    return content


def main(url, logger):
    """Run pipeline section

    Args:
        url (string): URL to .csv of cik ticker matching
        logger (logger): logger to handle errors/info
    """

    f_original = download(url)
    f_repaired = repair_file(f_original)
    return f_repaired
