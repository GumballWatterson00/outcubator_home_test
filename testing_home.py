from zipfile import ZipFile
import re
import requests

download_urls = [ "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
]

url = download_urls[0]

def get_s3_file_name(url):
    name = re.findall('(?<=s3.amazonaws.com\/).*', url)
    return name[0]
    
r = requests.get(url, allow_redirects=True)

file_name = '/Users/mac/Python_SQL/' + get_s3_file_name(url)

open(file_name, 'wb').write(r.content)

with ZipFile(file_name, 'r') as zip:
    # printing all the contents of the zip file
    zip.printdir()
  
    # extracting all the files
    print('Extracting all the files now...')
    zip.extractall()
    print('Done!')

