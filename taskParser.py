import json
import gzip
import requests

from s3_service import upload_file

url = '127.0.0.1/v2/customers'
response = requests.get(url)
input_json = response.json()
output_json = {}
embedded_data = input_json['_embedded']
customers_data = embedded_data['customers']
for customers in customers_data:
    for customers_property in customers:
        propValue = customers[customers_property]
        if not isinstance(propValue, dict) and customers_property != 'background':
            output_json[customers_property] = propValue
        else:
            embedded = customers['_embedded']
            links = customers['_links']
            fields_for_links = ['chats', 'websites', 'phones', 'social-profiles']
            for el in embedded:
                if embedded[el]:
                    emails_embedded = embedded[el][0]
                    for i in emails_embedded:
                        output_json[el + '_' + i] = emails_embedded[i]
            for l in fields_for_links:
                output_json[l] = links[l]['href']


with gzip.GzipFile('output_file', 'w') as fout:
    fout.write(json.dumps(output_json).encode('utf-8'))

upload_file(file_name='output_file', bucket='elementor-test-exercise')