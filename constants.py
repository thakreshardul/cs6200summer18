import sys

#############
# SEED URLS #
#############
SEEDS = ['http://www.nhc.noaa.gov/outreach/history/',
         'https://en.wikipedia.org/wiki/List_of_United_States_hurricanes',
         'http://en.wikipedia.org/wiki/Effects_of_Hurricane_Sandy_in_New_York',
         'http://en.wikipedia.org/wiki/Hurricane_Sandy',
         'https://www.cnn.com/2013/07/13/world/americas/hurricane-sandy-fast-facts',
         'https://www.britannica.com/event/Superstorm-Sandy']

INITIAL_PRIORITY = -1 * sys.maxsize

KEYWORDS = ['hurricane', 'cyclones', 'storm', 'blizzard', 'sandstorm', 'super-storm', 'tropical',
            'sandy', 'hazard', 'typhoon', 'alarming', 'tornado']


###################
# INDEX CONSTANTS #
###################
INDEX = 'crawl_dataset'
DOC_TYPE = 'document'


#################
# MISCELLANEOUS #
#################
TOTAL_DOCUMENTS = 21000
TEXT_ENCODING = 'utf-8'
URL_TIMEOUT = 5
HTML_PARSER = 'html.parser'
HOST = 'localhost'
PORT = 9200
DATASET_PATH = '/Users/maverick/CS6200/Homeworks/HW3/test/*'
IN_LINKS = '/Users/maverick/CS6200/Homeworks/HW3/in_links'
