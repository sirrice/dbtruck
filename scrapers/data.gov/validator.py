import re
import tempfile

import requests

def test_datafile(filename):
	'''Test our data file to see if DBTruck successfully ingests it'''

	matcher = re.compile('^(?P<name>.*),(?P<url>http://.*)$')
	formats = re.compile('.*(?P<format>csv|xls|txt).*', re.I)

	with open(filename) as datafile:

		for line in datafile:
			matches = matcher.match(line.strip())
			url = matches.group('url')

			format = formats.match(url).group('format')
			request = requests.get(url)

			if request.ok:
				
				#try_ingest(request.txt)


def persist_data(data, format):
	'''Save retrieved data to a temp file, return it and its pathname'''
	handle, pathname = tempfs.mktmp(suffix=format)




def try_ingest(data_file):
	'''Try to ingest our data file'''
	parsers = dbtruck.parsers.get_readers(data_file)



def main():
	test_datafile('./scrapers/data.gov/data.txt')


if __name__ == '__main__':
	main()
