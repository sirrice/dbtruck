import urllib2
import re
from bs4 import BeautifulSoup as Soup


ROOT = 'http://explore.data.gov'
FORMATS = ['CSV', 'XLS', 'KML', 'ESRI', 'JSON', 'PDF', 'RSS', 'RDF', 'XML']


def load_catalog(page=1):

    document = urllib2.urlopen('{0}/catalog/raw?&page={1}'.format(ROOT, page))
    html = document.read()

    return Soup(html)


def get_page_links(soup):

    links = soup.find_all('a', {'class': 'name'})

    return map(lambda link: {'url': link.attrs['href'],
                             'name': link.text},
               links)


def download_data(page_url):

    document = urllib2.urlopen(ROOT + page_url)
    html = document.read()

    soup = Soup(html)
    download_string = "/download/.{9}/(" + \
                      "|".join(FORMATS) + ")"

    download_re = re.compile(download_string, re.I)

    downloads = soup.find_all(href=download_re)

    if len(downloads) == 0:
        # must be the export case.
        number = page_url[-9:]
        url = 'http://explore.data.gov/api/views/{0}/rows.csv?accessType=DOWNLOAD'.format(number)
    else:
        url = ROOT + downloads[0].attrs['href']

    return url


if __name__ == '__main__':

    with open('data.txt', 'w') as output:

        for i in range(1, 150):

            catalog = load_catalog(i)
            links = get_page_links(catalog)

            for link in links:

                data_url = download_data(link['url'])
                line = ",".join([link['name'], data_url])
                line += "\n"

                output.write(line)
