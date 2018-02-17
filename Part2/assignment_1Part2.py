
# coding: utf-8

# In[1]:



##### Assignement-1 --Part 2 --Team 11

############### Import Libraries ###############
import urllib.request
import zipfile
import os
import pandas as pd
import logging # for logging
import shutil #to delete the directory contents
import glob
import boto.s3
import sys
from boto.s3.key import Key
import time
import datetime

############### Initializing logging file ###############

logger= logging.getLogger()
logger.setLevel(logging.DEBUG)
#fh is file header
fh = logging.FileHandler('Problem2_log.log') #output the logs to a file
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


############### Cleanup required directories ###############

try:
    if not os.path.exists('downloaded_zips'):
        os.makedirs('downloaded_zips', mode=0o777)
    else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips'), ignore_errors=False)
        os.makedirs('downloaded_zips', mode=0o777)
    
    if not os.path.exists('downloaded_zips_unzipped'):
        os.makedirs('downloaded_zips_unzipped', mode=0o777)
    else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__), 'downloaded_zips_unzipped'), ignore_errors=False)
        os.makedirs('downloaded_zips_unzipped', mode=0o777)
    logging.info('Directories cleanup complete.')
except Exception as e:
    logging.error(str(e))
    exit()     
    
############### Function to Download zips ###############
def download_zip(url):
    zips = []
    try:
        zips.append(urllib.request.urlretrieve(url, filename= 'downloaded_zips/'+url[-15:]))
        if os.path.getsize('downloaded_zips/'+url[-15:]) <= 4515: #catching empty file
            os.remove('downloaded_zips/'+url[-15:])
            logging.warning('Log file %s is empty. Attempting to download for next date.', url[-15:])
            return False
        else:
            logging.info('Log file %s successfully downloaded', url[-15:])
            return True
    except Exception as e: #Catching file not found
        logging.warning('Log %s not found...Skipping ahead!', url[-15:])
        return True

############## Fetch all the command line arguments ###################
year = input('Enter Year')
CIK = input('Enter CIK Number  ') #CIK number is inputed
acc_no = input('Enter the accession number ') #Account_number is inputed

# method to use acc_no as input to create accession_number
def dash(string):
    if (len(string) == 18):
        return string[:10] + "-" + string[10:12] + "-" + string[12:] # dashes are inputed at right index values to create accession_number 
    else:
        print("Values are invalid")
        
accession_no = dash(acc_no)


############### Validate amazon keys ###############


accessKey = input("Enter AccessKey :")
secretAccessKey= input("Enter SecretAccessKey: ")
location= input("Enter Location: ")

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
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()


############### Generate URLs and download zip for the inputted year ###############

URL = "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"
qtr_months = {'Qtr1':['01','02','03'], 'Qtr2':['04','05','06'], 'Qtr3':['07','08','09'], 'Qtr4':['10','11','12']}
valid_years = range(2003,2017)
days = range(1,32)

if not year:
    year = 2003
    logging.warning('Program running for 2003 by default since you did not enter any Year.')

if int(year) not in valid_years:
    logging.error("Invalid year. Please enter a valid year between 2003 and 2016.")
    exit()

logging.info('Initializing zip download.')

url_final = []
for key, val in qtr_months.items():
    for v in val:
        for d in days:
            url = URL +str(year) +'/' +str(key) +'/' +'log' +str(year) +str(v) + str(format(d,'02d')) +'.zip'
            if download_zip(url):
                break
            else:
                continue
logging.info('All log files downloaded for %s', year)


############### Unzip the logs and extract csv ###############
try:
    zip_files = os.listdir('downloaded_zips')
    for f in zip_files:
        z = zipfile.ZipFile(os.path.join('downloaded_zips', f), 'r')
        for file in z.namelist():
            if file.endswith('.csv'):
                z.extract(file, r'downloaded_zips_unzipped')
    logging.info('Zip files successfully extracted to folder: downloaded_zips_unzipped.')
except Exception as e:
        logging.error(str(e))
        exit()


############### Load the csvs into dataframe ###############
try:
    filelists = glob.glob('downloaded_zips_unzipped' + "/*.csv")
    all_csv_df_dict = {period: pd.read_csv(period) for period in filelists}
    logging.info('All the csv read into individual dataframes')
except Exception as e:
    logging.error(str(e))
    exit()
                   
                   
############### The following section deals with DETECTING ANOMALIES, ###############
############### HANDLING MISSING VALUES and computing ###############################
############### SUMMARY METRICS for one dataframe at a time #########################

try:
    for key, val in all_csv_df_dict.items():
        df = all_csv_df_dict[key]
        #detecting null values
        null_count = df.isnull().sum()
        logging.info('Count of Null values for %s in all the variables:\n%s ', key, null_count)
        
        # variable idx should be either 0 or 1
        incorrect_idx = (~df['idx'].isin([0.0,1.0])).sum()
        logging.info('There are %s idx which are not 0 or 1 in the log file %s', incorrect_idx, key) 
        
        # variable norefer should be either 0 or 1
        incorrect_norefer = (~df['norefer'].isin([0.0,1.0])).sum()
        logging.info('There are %s norefer which are not 0 or 1 in the log file %s', incorrect_norefer, key) 
        
        # variable noagent should be either 0 or 1
        incorrect_noagent = (~df['noagent'].isin([0.0,1.0])).sum()
        logging.info('There are %s noagent which are not 0 or 1 in the log file %s', incorrect_noagent, key) 
        
        #remove rows which have no ip, date, time, cik or accession
        df.dropna(subset=['cik'])
        df.dropna(subset=['accession'])
        df.dropna(subset=['ip'])
        df.dropna(subset=['date'])
        df.dropna(subset=['time'])
        
        #replace nan with the most used browser in data.
        max_browser = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
        df['browser'] = df['browser'].fillna(max_browser)
        
        # replace nan idx with max idx
        max_idx = pd.DataFrame(df.groupby('idx').size().rename('cnt')).idxmax()[0]
        df['idx'] = df['idx'].fillna(max_idx)
        
        # replace nan code with max code
        max_code = pd.DataFrame(df.groupby('code').size().rename('cnt')).idxmax()[0]
        df['code'] = df['code'].fillna(max_code)
        
        # replace nan norefer with zero
        df['norefer'] = df['norefer'].fillna('1')
        
        # replace nan noagent with zero
        df['noagent'] = df['noagent'].fillna('1')
        
        # replace nan find with max find
        max_find = pd.DataFrame(df.groupby('find').size().rename('cnt')).idxmax()[0]
        df['find'] = df['find'].fillna(max_find)
        
        # replace nan crawler with zero
        df['crawler'] = df['crawler'].fillna('0')
        
        # replace nan extention with max extention
        max_extention = pd.DataFrame(df.groupby('extention').size().rename('cnt')).idxmax()[0]
        df['extention'] = df['extention'].fillna(max_extention)
        
        # replace nan extention with max extention
        max_zone = pd.DataFrame(df.groupby('zone').size().rename('cnt')).idxmax()[0]
        df['zone'] = df['zone'].fillna(max_zone)
    
        # find mean of the size and replace null values with the mean
        df['size'] = df['size'].fillna(df['size'].mean(axis=0))
        
        ##### Summary Metrics #####
        #Compute mean size
        df['size_mean'] = df['size'].mean(axis=0)
        
        #Compute maximum used browser
        df['max_browser'] = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
        
        #Compute distinct count of ip per month i.e. per log file
        df['ip_count'] = df['ip'].nunique()
        
        #Compute distinct count of cik per month i.e. per log file
        df['cik_count'] = df['cik'].nunique()
    
    logging.info('Rows removed where ip, date, time, cik or accession were null.')
    logging.info('NaN values in browser replaced with maximum count browser.')
    logging.info('NaN values in idx replaced with maximum count idx.')
    logging.info('NaN values in code replaced with maximum count code.')
    logging.info('NaN values in norefer replaced with 0.')
    logging.info('NaN values in noagent replaced with 0.')
    logging.info('NaN values in find replaced with maximum count find.')
    logging.info('NaN values in crawler replaced with 0.')
    logging.info('NaN values in extension replaced with maximum count extension.')
    logging.info('NaN values in zone replaced with maximum count zone.')
    logging.info('NaN values in size replaced with mean value of size.')
    logging.info('New column added to dataframe: Mean of size.')
    logging.info('New column added to dataframe: Max count of browser.')
    logging.info('New column added to dataframe: Count of distinct ip per month.')
    logging.info('New column added to dataframe: Count of distinct cik per month.')
except Exception as e:
    logging.error(str(e))
    exit()
    
############### Combining all dataframe and computing overall summary metric ###############
# writing csv for all data
try:
    master_df = pd.concat(all_csv_df_dict)
    master_df.to_csv('main_csv.csv')
    logging.info('All dataframes of csvs are combined and exported as csv: main_csv.csv.')
except Exception as e:
    logging.error(str(e))
    exit()
    
# write csv for summary of combined data.
#try:
#    master_df_summary = master_df.describe()
#    master_df_summary.to_csv('master_df_summary.csv')
#    logging.info('The summary metric of combined csv is generated and exported as csv: master_df_summary.csv .')
#except Exception as e:
#    logging.error(str(e))
#   exit()

############### Zip the csvs and logs ###############
def zipdir(path, ziph):
    ziph.write(os.path.join('main_csv.csv'))
#    ziph.write(os.path.join('master_df_summary.csv'))
    ziph.write(os.path.join('Problem2_log.log'))   

zipf = zipfile.ZipFile('Problem2.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf)
zipf.close()
logging.info('Compiled csv and log file zipped')
    
############### Upload the zip to AWS S3 ###############
############### Fetch the location argument if provided, else user's system location is taken ############### 
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
    bucket_name = AWS_ACCESS_KEY_ID.lower()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","")
    bucket = conn.create_bucket(bucket_name, location=loc)
    print("bucket created")
    zipfile = 'Problem2.zip'
    print ("Uploading %s to Amazon S3 bucket %s", zipfile, bucket_name)
    
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()
    
    k = Key(bucket)
    k.key = 'Problem2'
    k.set_contents_from_filename(zipfile,
        cb=percent_cb, num_cb=10)
    print("Zip File successfully uploaded to S3")
except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()
############ EOF ############

