
# coding: utf-8

# In[ ]:



############### PROBLEM 1 PART 1 ############### 
#Assignmnet_1 -- Part 1 Team 11

############### Import Libraries ###############
import urllib.request
from bs4 import BeautifulSoup #for web scraping
import csv #for writing csv
import logging #for logging
import os
import zipfile
import boto.s3
import sys
from boto.s3.key import Key
import time
import datetime


############### Initializing logging file ###############

logger= logging.getLogger()
logger.setLevel(logging.DEBUG)

#fh is file header
fh = logging.FileHandler('Problem1_log.log') #output the logs to a file
fh.setLevel(logging.DEBUG) #setting loglevel to DEBUG
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') #format for the output
fh.setFormatter(formatter)
logger.addHandler(fh)

#ch is console header
ch = logging.StreamHandler(sys.stdout ) #print the logs in console as well
ch.setLevel(logging.DEBUG) #setting loglevel to DEBUG
formatter = logging.Formatter('%(levelname)s - %(message)s') #format for the output
ch.setFormatter(formatter)
logger.addHandler(ch)


############## Input Values ########################


CIK = input('Enter CIK Number  ') #CIK number is inputed
acc_no = input('Enter the accession number ') #Account_number is inputed

# method to use acc_no as input to create accession_number
def dash(string):
    if (len(string) == 18):
        return string[:10] + "-" + string[10:12] + "-" + string[12:] # dashes are inputed at right index values to create accession_number 
    else:
        print("Values are invalid")
        
accession_no = dash(acc_no)

fixedURL = "https://www.sec.gov"

URL = (fixedURL + "/Archives/edgar/data/"+ CIK+"/" + acc_no + "/"+ accession_no +"-index.html")
print(URL)

externalList = []

soup = BeautifulSoup((urllib.request.urlopen(URL)),"html.parser")# html parser
for t in soup.find_all('table' , attrs={"summary": "Document Format Files"}):
     for tr in soup.find_all('tr'):
        for td in tr.findChildren('td'):
            if(td.text == '10-Q'):
                for a in tr.findChildren('a', href=True):
                    href = (a['href'])

URL = fixedURL + href                    
print(URL)

logger.info("URL for the 10-q file for CIK = %s and Accession_no = %s is created", CIK,acc_no)
            
                  
#print(finalURL)



tablelist = []

soup = BeautifulSoup((urllib.request.urlopen(URL)),"html.parser")
tables = soup.find_all('table')
for table in tables: 
    for tr in table.find_all('tr'):
        i = 0
        for td in tr.findChildren('td'):
            if ("background" in str(td.get('style'))):
                tablelist.append(table)
                i = 1
                break
        if(i == 1):
            break



############### Validate amazon keys ###############
#Input amazon accesskey, secretAccessKey and location

accessKey = input('Enter Access key')
secretAccessKey =input('Secret Enter Access key')
location = input('Enter location')

#condition to chek if the accessKey & secretAccesskey is valid or not

if not accessKey or not secretAccessKey:
    logging.warning('Access Key and Secret Access Key not provided!!')
    print('Access Key and Secret Access Key not provided!!')
    exit()

AWS_ACCESS_KEY_ID = accessKey
AWS_SECRET_ACCESS_KEY = secretAccessKey
inputLocation = location

try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY)

    print("Connected to S3")

except:
    logging.info("Amazon keys are invalid")
    print("Amazon keys are invalid")
    exit()


############### Access the form and fetch the tables ###############

if not os.path.exists('extractedFiles_csvs'):
    os.makedirs('extractedFiles_csvs')

page = urllib.request.urlopen(URL)
soup = BeautifulSoup(page,"lxml")
all_tables = soup.select('div table')

############### Fetch data which contains '$' or '%' ###############

refined_tables=[]

for tab in all_tables:
    for tr in tab.find_all('tr'):
        f=0
        for td in tr.findAll('td'):
            if('$' in td.get_text() or '%' in td.get_text()):
                refined_tables.append(tab)
                f=1;
                break;
        if(f==1):
            break;    
            
############### For all the refined tables, following is performed ##############
############### Fetching the data inside <td> tags and removing characters such as '\n','\xa0' ###############
############### After cleaning, writing the <td> into a csv file ###############

for tab in refined_tables:
    records = []
    for tr in tab.find_all('tr'):
        rowString=[]
        for td in tr.findAll('td'):
            p = td.find_all('p')
            if len(p)>0:
                for ps in p:
                    ps_text = ps.get_text().replace("\n"," ") 
                    ps_text = ps_text.replace("\xa0","")                 
                    rowString.append(ps_text)
            else:
                td_text=td.get_text().replace("\n"," ")
                td_text = td_text.replace("\xa0","")
                rowString.append(td_text)
        records.append(rowString)        
    with open(os.path.join('extractedFiles_csvs' , str(refined_tables.index(tab)) + 'tables.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(records)
        
logging.info('Tables successfully extracted to csv') 
    
###############################Zip the csvs and logs ###############################

def zipdir(path, zipfh, refined_tables):
    # zipfh is zipfile handle
    for tab in refined_tables:
        zipfh.write(os.path.join('extractedFiles_csvs', str(refined_tables.index(tab))+'tables.csv'))
    zipfh.write(os.path.join('Problem1_log.log'))   #extracting log values
#zipf is zipfile
zipf = zipfile.ZipFile('Problem1.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf, refined_tables)
zipf.close()
logging.info('csv and log file zipped')


############################### Upload the zip to AWS S3 ###############################
#condition to check if the location exists or no
loc=''

if inputLocation == 'APNortheast':
    loc=boto.s3.connection.Location.APNortheast
elif inputLocation == 'APSoutheast':
    loc=boto.s3.connection.Location.APSoutheast
elif inputLocation == 'APSoutheast2':
    loc=boto.s3.connection.Location.APSoutheast2
elif inputLocation == 'CNNorth1':
    loc=boto.s3.connection.Location.CNNorth1
elif inputLocation == 'EUCentral1':
    loc=boto.s3.connection.Location.EUCentral1
elif inputLocation == 'EU':
    loc=boto.s3.connection.Location.EU
elif inputLocation == 'SAEast':
    loc=boto.s3.connection.Location.SAEast
elif inputLocation == 'USWest':
    loc=boto.s3.connection.Location.USWest
elif inputLocation == 'USWest2':
    loc=boto.s3.connection.Location.USWest2
try:   
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts)    
    #bucket_name = AWS_ACCESS_KEY_ID.reverse()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","") 
    bucket_name = AWS_ACCESS_KEY_ID.lower()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","") 
    bucket = conn.create_bucket(bucket_name, location=loc)
    print("bucket created")
    zipfile = 'Problem1.zip'
    print ("Uploading %s to Amazon S3 bucket %s", zipfile, bucket_name)
    
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()
    
    k = Key(bucket)
    k.key = 'Problem1'
    k.set_contents_from_filename(zipfile,
        cb=percent_cb, num_cb=10)
    print("Zip File successfully uploaded to S3")
except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()

###############################END OF CODE###############################E

