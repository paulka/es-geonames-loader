import csv
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

csv.field_size_limit(sys.maxsize)

es = Elasticsearch(['192.168.59.3'])

with open('datafiles/US.txt', 'rb') as geonamesfile:
  geonamesreader = csv.reader(geonamesfile, delimiter='\t')
  for row in geonamesreader:
    print row[1]
    doc = {'name': row[1],
           'ascii_name': row[2],
           'alternate_names': row[3],
           'lat': row[4],
           'long': row[5],
           'feature_class': row[6],
           'feature_code': row[7],
	   'country_code': row[8],
           'cc2': row[9],
	   'admin1_code': row[10],
	   'admin2_code': row[11],
	   'admin3_code': row[12],
	   'admin4_code': row[13],
	   'population': row[14],
	   'elevation': row[15],
	   'dem': row[16],
	   'timezone': row[17],
	   'modification_date': row[18]
    }
    res = es.index(index="geonames-001", doc_type='geoname', id=row[0], body=doc)
    print(res)
