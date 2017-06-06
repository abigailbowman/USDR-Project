import json
import re

import requests
import pandas as pd
import twitter   # This is @bear's Python-Twitter wrapper: https://github.com/bear/python-twitter
from twitter import models

import settings  # Be sure to add your Twitter API consumer key/secret to settings.py or this won't work

# fetch all social media records from USDR API
def fetchUSDR():
    d = json.loads(requests.get("https://api.gsa.gov/systems/digital-registry/v1/social_media.json").text)
    num_pages = d['metadata']['pages']
    results = d['results']
    
    # paginate through rest of results and add to list
    for page in range(2, num_pages+1):
        d = json.loads(requests.get("https://api.gsa.gov/systems/digital-registry/v1/social_media.json", params={'page': page}).text)
        results += d['results']
        
    # save results to a json txt file for later reference
    with open('data/USDR_accts.txt', 'w+') as file:
        json.dump(results, file)
    
    # create a pandas dataframe from results
    df = pd.DataFrame(results)
    
    # count how many accounts were fetched
    print("# of accounts fetched: " + "{:,}".format(len(results)))
    
    # list platforms by # of accts
    print(df['service_key'].value_counts())
    
    return df

# load USDR social media records from json txt file
def loadUSDR():
    with open('data/USDR_accts.txt', 'r') as file:
        results = json.load(file)
        
    df = pd.DataFrame(results)
    
    return df

def check_missing_screen_name(service_key, service_url):
    if service_key not in service_url:
        return "service_key does not match service_url"
    else: return "missing screen name (check service_url for errors)"

# since not all entries have usernames filled in, parse a Twitter URL for screen name and convert to lower case
def getTwitterUsername(url):
    regex = r"(?:https?:\/\/)?(?:www\.)?[tT]witter\.com\/(?:#!\/)?@?([^\/\?\s]*)"
    try:
        username = re.search(regex, url).group(1)
        return username.lower()
    except AttributeError:
        return float('NaN')

# need this for Twitter API calls to UsersLookup
def chunks(biglist, chunksize):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(biglist), chunksize):
        yield biglist[i:i + chunksize]

def fetchTwitter(username_list):
    # remove empty strings and de-dupe username list 
    username_list = list(filter(None, username_list))
    username_list = list(set(username_list))
    
    total_items = len(username_list)
    chunked_list = list(chunks(username_list, 100))
    total_chunks = len(chunked_list)
    
    # Set up API call using keys/secrets from settings.py
    api = twitter.Api(consumer_key = settings.twitter_consumer_key,
                  consumer_secret = settings.twitter_consumer_secret,
                  access_token_key = settings.twitter_access_key,
                  access_token_secret = settings.twitter_access_secret,
                  sleep_on_rate_limit = True)
    count = 1
    results = []
    
    print('Calling Twitter API for ' + str(total_items) + ' screen names in ' + str(total_chunks) + ' chunks...')
    
    for chunk in chunked_list:
        print('Processing chunk ' + str(count) + ' of ' + str(total_chunks))
        a = api.UsersLookup(screen_name = chunk)
        for result in a:
            results.append(result.AsDict())
        count += 1
    
    total_results = len(results)
    print('Found information for ' + str(total_results) + ' screen names.')

    # save results to a json txt file for later reference
    with open('data/Twitter_API_Results.txt', 'w+') as file:
        json.dump(results, file)
        
    df = pd.DataFrame(results)
    
    return df

def loadTwitter():
    with open('data/Twitter_API_Results.txt', 'r') as file:
        results = json.load(file)
        
    df = pd.DataFrame(results)
    
    return df

def getLastTweet(tweet_obj):
    try:
        return tweet_obj['created_at']
    except TypeError:
        return float('NaN')

def lastTweetedCategory(datetime_obj):
    diff = pd.datetime.now() - datetime_obj
    if diff < pd.Timedelta('24 hours'):
        return 'within last 24 hours'
    elif diff < pd.Timedelta('7 days'):
        return 'within last week'
    elif diff < pd.Timedelta('30 days'):
        return 'within last month'
    elif diff < pd.Timedelta('365 days'):
        return 'within last year'
    else:
        return 'more than a year ago'

#####

# turn off 'SettingWithCopyWarning' error message in pandas
pd.options.mode.chained_assignment = None  # default='warn'

accts = loadUSDR()  # First time, use:  accts = fetchUSDR()

accts[['created_at','updated_at']] = accts[['created_at','updated_at']].apply(pd.to_datetime)

# filter list of accounts to just Twitter
twitter_accts = accts[accts["service_key"] == "twitter"]

# get lowercase screen name from URL
twitter_accts['screen_name'] = twitter_accts['service_url'].apply(lambda x: getTwitterUsername(x))

#print('Missing \"account\" values: ' + str(twitter_accts['account'].isnull().sum()))
#print('Missing \"screen_name\" values: ' + str(twitter_accts['screen_name'].isnull().sum()))

# find duplicates/missing fields and save all (including first occurence) to data frame for future checking
twitter_accts['has_duplicate'] = twitter_accts.duplicated(['screen_name'], keep = False)
dupes = twitter_accts[twitter_accts['has_duplicate'] == True]
dupes['error'] = 'screen name is not unique'
#print('Size of acct_errors: ' + str(dupes.shape))

missing_account_names = twitter_accts[twitter_accts['account'].isnull()]
missing_account_names['error'] = 'missing account field (use screen_name if available)'
#print('Size of missing_account_names: ' + str(missing_account_names.shape))

missing_screen_names = twitter_accts[twitter_accts['screen_name'].isnull()]
missing_screen_names['error'] = missing_screen_names['service_url'].apply(lambda x: check_missing_screen_name('twitter', x))
#print('Size of missing_screen_names: ' + str(missing_screen_names.shape))

errors = pd.concat([missing_screen_names, missing_account_names, dupes], ignore_index=True)
#print('Size of combined \'errors\' data frame: ' + str(errors.shape))

#print(errors['error'].value_counts())

# re-do duplicate search while preserving the most recently added entries as non-dupes
# twitter_accts.sort_values('created_at', ascending = False, inplace = True)
# twitter_accts['is_duplicated'] = twitter_accts.duplicated(['screen_name'], keep = 'first')

# call Twitter API and get user IDs and other metadata for column of screen names
username_list = twitter_accts['screen_name'].tolist()

twitter_api = loadTwitter()  # First time (or when you want to update), use:  twitter_api = fetchTwitter(username_list)

twitter_api['last_tweeted_at'] = twitter_api['status'].apply(lambda x: getLastTweet(x))

twitter_api[['last_tweeted_at', 'created_at']] = twitter_api[['last_tweeted_at', 'created_at']].apply(pd.to_datetime)

twitter_api['last_tweeted_category'] = twitter_api['last_tweeted_at'].apply(lambda x: lastTweetedCategory(x))

twitter_api['screen_name_capitalized'] = twitter_api['screen_name']
twitter_api['screen_name'] = twitter_api['screen_name'].apply(lambda x: str.lower(x))

# Merge the two datasets on the lower-case screen name field
twitter_merged = pd.merge(twitter_accts, twitter_api, how='outer', on=None, left_on='screen_name', right_on='screen_name', left_index=False, right_index=False, sort=True, suffixes=('_usdr', '_api'), copy=False, indicator=False)

# run stats on results
total_usdr_records = twitter_merged['id_usdr'].nunique()
print("{:<38}".format('Total USDR Twitter records: ') + "{:>6,}".format(total_usdr_records))

total_screen_names = twitter_merged['screen_name'].nunique()
print("{:<38}".format('Unique Twitter usernames: ') + "{:>6,}".format(total_screen_names) + " (" + '{:.1%}'.format(total_screen_names/total_usdr_records) + " of total USDR Twitter records)")

total_twitter_ids = twitter_merged['id_api'].nunique()
print("{:<38}".format('User records found using Twitter API: ') + "{:>6,}".format(total_twitter_ids) + " (" + '{:.1%}'.format(total_twitter_ids/total_screen_names) + " of unique Twitter screen names)")

# create dataframe of only the USDR entries with API results; only keep most recent entry if duplicate
twitter_merged_unique = twitter_merged.sort_values('created_at_usdr', ascending = False)
twitter_merged_unique.dropna(subset=['id_api'], inplace = True)
twitter_merged_unique.drop_duplicates(subset=['id_api'], keep = 'first', inplace = True)

verified_accts = twitter_merged_unique['verified'].sum()
print("{:<38}".format('Verified users (with checkmark): ') + "{:>6,}".format(verified_accts) + " (" + '{:.1%}'.format(verified_accts/total_twitter_ids) + " of records found using Twitter API)\n")

print('USER\'S MOST RECENT TWEET BY CATEGORY:')

dayago = (twitter_merged_unique['last_tweeted_category'] == 'within last 24 hours').sum()
print("{:<38}".format('Less than 24 hours ago:') + "{:>6,}".format(dayago) + " (" + '{:.1%}'.format(dayago/total_twitter_ids) + " of records found using Twitter API)")

weekago = (twitter_merged_unique['last_tweeted_category'] == 'within last week').sum()
print("{:<38}".format('Within the last week: ') + "{:>6,}".format(weekago) + " (" + '{:.1%}'.format(weekago/total_twitter_ids) + ")")

monthago = (twitter_merged_unique['last_tweeted_category'] == 'within last month').sum()
print("{:<38}".format('Within the last month: ') + "{:>6,}".format(monthago) + " (" + '{:.1%}'.format(monthago/total_twitter_ids) + ")")

yearago = (twitter_merged_unique['last_tweeted_category'] == 'within last year').sum()
print("{:<38}".format('Within the last year: ') + "{:>6,}".format(yearago) + " (" + '{:.1%}'.format(yearago/total_twitter_ids) + ")")

morethanayear = (twitter_merged_unique['last_tweeted_category'] == 'more than a year ago').sum()
print("{:<38}".format('More than a year ago: ') + "{:>6,}".format(morethanayear) + " (" + '{:.1%}'.format(morethanayear/total_twitter_ids) + ")")