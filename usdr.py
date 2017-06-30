import json
import re

import requests
import pandas as pd
import twitter   # This is @bear's Python-Twitter wrapper: https://github.com/bear/python-twitter
import facebook  # This is @mobolic's Facebook-SDK wrapper: https://github.com/mobolic/facebook-sdk

import settings  # Be sure to add your platform API consumer keys/secrets to settings.py or this won't work

''' MAIN ESSENTIAL FUNCTIONS '''

# fetch all social media records from USDR API
def fetchUSDR():
    d = requests.get("https://api.gsa.gov/systems/digital-registry/v1/social_media.json").json()
    num_pages = d['metadata']['pages']
    results = d['results']
    
    # paginate through rest of results and add to list
    for page in range(2, num_pages+1):
        d = requests.get("https://api.gsa.gov/systems/digital-registry/v1/social_media.json", params={'page': page}).json()
        try:
            results += d['results']
        except:
            print('Rate limit may have been exceeded; wait and try again later.')
            break
        
    # save results to a json txt file for later reference
    with open('data/USDR_accts.json', 'w+') as file:
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
    with open('data/USDR_accts.json', 'r') as file:
        results = json.load(file)
        
    df = pd.DataFrame(results)
    
    return df

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
        print('\rProcessing chunk ' + str(count) + ' of ' + str(total_chunks) + "  ", end='')
        a = api.UsersLookup(screen_name = chunk)
        for result in a:
            results.append(result.AsDict())
        count += 1
    
    total_results = len(results)
    print('\rFound information for ' + str(total_results) + ' screen names.')

    # save results to a json txt file for later reference
    with open('data/Twitter_API_Results.json', 'w+') as file:
        json.dump(results, file)
        
    df = pd.DataFrame(results)
    
    df['last_api_call'] = pd.datetime.now()
    
    return df

def loadTwitter():
    with open('data/Twitter_API_Results.json', 'r') as file:
        results = json.load(file)
        
    df = pd.DataFrame(results)
    
    return df

def fetchFacebook(url_list):
    df_urls = fetchFacebookURLs(url_list)
    
#==============================================================================
#     notfound = df_urls[df_urls['name'].isnull()]
#     notfound_urls = notfound['url'].tolist()
#     
#     notfound_urls = list(filter(None, notfound_urls))
#     notfound_urls = list(set(notfound_urls))
#     total_notfound_urls = len(notfound_urls)
#     
#     new_urls = []
#     count = 1
#     
#     print('Checking redirects for ' + str(total_notfound_urls) + ' URLs...')
#     
#     with requests.Session() as s:
#         for old_url in notfound_urls:
#             print('\rProcessing URL ' + str(count) + ' of ' + str(total_notfound_urls) + "    ", end='')
#             if old_url == None:
#                 continue
#             try:
#                 resp = s.head(old_url, allow_redirects=True)
#             except:
#                 if 'http' not in old_url:
#                     old_url = 'http://' + old_url
#                 if ('facebook.com' not in old_url and get_username(old_url) != None):
#                     old_url = 'http://www.facebook.com/' + str(get_username(old_url))
#                 try:
#                     resp = s.head(old_url, allow_redirects=True)
#                 except:
#                     continue
#             new_urls.append(resp.url)
#             count += 1
#      
#     new_unique_urls = list(set(new_urls) & set(notfound_urls))
#     
#     print('\rFound ' + str(len(new_unique_urls)) + ' new URLs             ')
#     
#     df_urls = pd.concat([df_urls, fetchFacebookURLs(new_unique_urls)], ignore_index=True)
#==============================================================================
    
    # save results to a json txt file for later reference
    with open('data/Facebook_API_Results_by_URL.json', 'w+') as file:
        json.dump(df_urls.to_json(), file)
        
    id_list = df_urls[df_urls['error'].isnull()]['id'].tolist()
    
    df_ids = fetchFacebookDetails(id_list)
    df_ids['last_api_call'] = pd.datetime.now()
    
    # save results to a json txt file for later reference
    with open('data/Facebook_API_Results_by_ID.json', 'w+') as file:
        json.dump(df_ids.to_json(), file)
    
    df_merged = pd.merge(df_urls, df_ids, how='outer', on='id', left_index=False, right_index=False, sort=True, suffixes=('_url', '_id'), copy=False, indicator=False)

    return df_merged

def loadFacebook():
    with open('data/Facebook_API_Results_by_URL.json', 'r') as file:
        url_results = json.load(file)
        
    df_urls = pd.DataFrame(url_results)
    
    with open('data/Facebook_API_Results_by_ID.json', 'r') as file:
        id_results = json.load(file)
        
    df_ids = pd.DataFrame(id_results)
    
    df_merged = pd.merge(df_urls, df_ids, how='outer', on='id', left_index=False, right_index=False, sort=True, suffixes=('_url', '_id'), copy=False, indicator=False)
    
    return df_merged


''' HELPER FUNCTIONS ''' 

# need this for Twitter API calls to UsersLookup
def chunks(biglist, chunksize):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(biglist), chunksize):
        yield biglist[i:i + chunksize]

def check_missing_screen_name(service_key_list, service_url):
    for service_key in service_key_list:
        if service_key in service_url:
            return "missing screen name (check service_url for errors)"
    return "service_key does not match service_url"

# since not all entries have usernames filled in, parse a Twitter URL for screen name and convert to lower case
def get_username(url):
    if 'witter.com' in url:
        regex = r"(?:https?:\/\/)?(?:www\.)?[tT]witter\.com\/(?:#!\/)?@?([^\/\?\s]*)"
    elif 'acebook.com' in url:
        # thanks to @marcgg and @nkanaev on GitHub thread: https://gist.github.com/marcgg/733592
        regex = r"(?:https?:\/\/)?(?:www\.)?[fF]acebook\.com\/(?:.+\/)*([\w\.\-]+)"  
    else:
        return None
    
    try:
        username = re.search(regex, url).group(1)
    except AttributeError:
        return None
    
    try:
        return username.lower()
    except:
        return username

def generate_url(row):
    if row['username'] and row['service_key'] == 'twitter':
        return 'https://www.twitter.com/' + row['username']
    elif row['username'] and row['service_key'] == 'facebook':
        return 'https://www.facebook.com/' + row['username']
    else:
        return None

def getLastTweet(tweet_dict):
    try:
        return tweet_dict['created_at']
    except TypeError:
        return None

def getLastFacebookPost(feed_dict):
    try:
        return feed_dict['data'][0]['created_time']
    except TypeError:
        return None    

def lastPostedCategory(datetime_difference):
    if datetime_difference < pd.Timedelta('24 hours'):
        return 'within last 24 hours'
    elif datetime_difference < pd.Timedelta('7 days'):
        return 'within last week'
    elif datetime_difference < pd.Timedelta('30 days'):
        return 'within last month'
    elif datetime_difference < pd.Timedelta('365 days'):
        return 'within last year'
    else:
        return 'more than a year ago'
    
def fetchFacebookDetails(id_list):
    # remove empty strings and de-dupe username list 
    id_list = list(filter(None, id_list))
    id_list = list(set(id_list))
    
    total_items = len(id_list)
    chunked_list = list(chunks(id_list, 50)) # FB API limits to 50 ids provided
    total_chunks = len(chunked_list)
    
    count = 1
    results = {}
    
    # set field list for details
    field_list = "about,can_checkin,category,category_list,checkins,contact_address,cover,description,display_subtext,displayed_message_response_time,emails,fan_count,featured_video,general_info,hours,is_always_open,is_community_page,is_eligible_for_branded_content,is_permanently_closed,is_unclaimed,is_verified,link,location,mission,name,name_with_location_descriptor,overall_star_rating,parent_page,phone,rating_count,talking_about_count,username,website,verification_status,feed.limit(1){created_time,story,status_type,id,permalink_url}"
    
    # first API call to find Facebook IDs for valid page/user URLs
    graph = facebook.GraphAPI(access_token=settings.facebook_access_token, version='2.7')
    
    print('Calling Facebook API for ' + str(total_items) + ' IDs in ' + str(total_chunks) + ' chunks...')

    for chunk in chunked_list:
        print('\rProcessing chunk ' + str(count) + ' of ' + str(total_chunks) + "    ", end='')
        while True:
            try:
                a = graph.get_objects(ids=chunk, fields=field_list)
                results.update(a)
                count += 1
                break
            except facebook.GraphAPIError as e:
                error_text = str(e)
                if 'Cannot query users by their username' in error_text:
                    error_list = error_text[error_text.rindex("(") + 1:error_text.rindex(")")].split(',')
                    print(str(len(error_list)) + ' username errors found:')
                    print(error_list)
                    for error in error_list:
                        chunk.remove(error)
                    print(str(len(chunk)) + ' items left in chunk')
                    continue
                elif 'Some of the aliases you requested do not exist' in error_text:
                    error_list = error_text[error_text.rindex(":")+2:].split(',')
                    print(str(len(error_list)) + ' username errors found:')
                    print(error_list)
                    for error in error_list:
                        chunk.remove(error)
                    print(str(len(chunk)) + ' items left in chunk')
                    continue
                else:
                    raise
    
    df = pd.DataFrame.from_dict(results, orient='index')
    
    total_results = len(results)
    
    print('\rFound information for ' + str(total_results) + ' IDs.') 
    
    return df

def fetchFacebookURLs(url_list):
    # remove empty strings, leading/trailing spaces, and de-dupe URL list 
    url_list = list(filter(None, url_list))
    url_list = list(set(url_list))
    
    total_items = len(url_list)
    chunked_list = list(chunks(url_list, 50)) # FB API limits to 50 ids provided
    total_chunks = len(chunked_list)
    
    count = 1
    results = {}
    
    # first API call to find Facebook IDs for valid page/user URLs
    graph = facebook.GraphAPI(access_token=settings.facebook_access_token, version='2.7')
    
    print('Calling Facebook API for ' + str(total_items) + ' URLs in ' + str(total_chunks) + ' chunks...')

    for chunk in chunked_list:
        print('\rProcessing chunk ' + str(count) + ' of ' + str(total_chunks) + "  ", end='')
        while True:
            try:
                a = graph.get_objects(ids=chunk)
                for details in a.values():
                    if 'name' in details:
                        details['is_valid'] = True
                        details['error'] = None
                    else:
                        details['is_valid'] = False
                        details['error'] = 'page is not available'
                results.update(a)
                count += 1
                break
            except facebook.GraphAPIError as e:
                error_text = str(e)
                if 'Cannot query users by their username' in error_text:
                    error_list = error_text[error_text.rindex("(") + 1:error_text.rindex(")")].split(',')
                    print(str(len(error_list)) + ' username errors found:')
                    print(error_list)
                    for error_url in error_list:
                        chunk.remove(error_url)
                        error_log = {error_url: {'error':'cannot query user by their username'}}
                        results.update(error_log)
                    print('Errors removed. ' + str(len(chunk)) + ' items left in chunk')
                    continue
                elif 'Some of the aliases you requested do not exist' in error_text:
                    error_list = error_text[error_text.rindex("exist:")+7:].split(',')
                    print(str(len(error_list)) + ' alias error found:')
                    print(error_list)
                    for error_url in error_list:
                        try:
                            chunk.remove(error_url)
                        except ValueError:
                            for url in chunk:
                                if url.strip() == error_url.strip():
                                    chunk.remove(url)
                                    break
                        error_log = {error_url: {'error':'the alias you requested does not exist'}}
                        results.update(error_log)
                    print('Errors removed. ' + str(len(chunk)) + ' items left in chunk')
                    continue
                else:
                    raise
        
    df = pd.DataFrame.from_dict(results, orient='index')
    
    total_results = len(results)
    valid_results = df['is_valid'].sum()
    
    print('\rFound information for ' + str(total_results) + ' URLs. ' + str(valid_results) + ' are valid Facebook pages.') 
    
    # URL as index will cause issues later; make URL its own column and reindex
    df.index.name='url'
    df.reset_index(inplace=True)
    
    return df

def print3col(a,b,c,d=''):   # assumes a is text, b and c are ints, and d is a percent, unless otherwise (in which case they're all strings)
    try:
        if (0 < b <= 1 and 0 < c <=1):
            print('{0:<30} {1:>10.1%} {2:>10.1%} {3}'.format(a,b,c,d))
        else:
            print('{0:<30} {1:>10,} {2:>10,} {3}'.format(a,b,c,d))
    except:
        print('{0:<30} {1:>10} {2:>10} {3}'.format(a,b,c,d))


''' TEST CODE ''' 

# turn off 'SettingWithCopyWarning' error message in pandas
pd.options.mode.chained_assignment = None  # default='warn'

'''
First time, use:  accts = fetchUSDR()
After, can use accts = loadUSDR()
'''
accts = fetchUSDR()

accts[['created_at','updated_at']] = accts[['created_at','updated_at']].apply(pd.to_datetime)

# filter list of accounts to respective platforms
#twitter_accts = accts[accts["service_key"] == "twitter"]
#facebook_accts = accts[accts["service_key"] == "facebook"]

# get lowercase screen name from URL
accts['username'] = accts['service_url'].apply(lambda x: get_username(x))

accts['url_from_username'] = accts.apply(generate_url, axis=1)

#print('Missing \"account\" values: ' + str(twitter_accts['account'].isnull().sum()))
#print('Missing \"screen_name\" values: ' + str(twitter_accts['screen_name'].isnull().sum()))

# find duplicates/missing fields and save all (including first occurence) to data frame for future checking
# twitter_accts['has_duplicate'] = twitter_accts.duplicated(['screen_name'], keep = False)
# facebook_accts['has_duplicate'] = facebook_accts.duplicated(['screen_name'], keep = False)

#dupes = twitter_accts[twitter_accts['has_duplicate'] == True]
#dupes = pd.concat([dupes, facebook_accts[facebook_accts['has_duplicate'] == True]], ignore_index=True)

#dupes['error'] = 'screen name is not unique'
#print('Size of acct_errors: ' + str(dupes.shape))

#missing_account_names = twitter_accts[twitter_accts['account'].isnull()]
#missing_account_names = pd.concat([missing_account_names, facebook_accts[facebook_accts['account'].isnull()]], ignore_index=True)
#missing_account_names['error'] = 'missing account field (use screen_name if available)'
#print('Size of missing_account_names: ' + str(missing_account_names.shape))

#missing_screen_names = twitter_accts[twitter_accts['screen_name'].isnull()]
#missing_screen_names = pd.concat([missing_screen_names, facebook_accts[facebook_accts['screen_name'].isnull()]], ignore_index=True)
#missing_screen_names['error'] = missing_screen_names['service_url'].apply(lambda x: check_missing_screen_name(['twitter','facebook'], x))
#print('Size of missing_screen_names: ' + str(missing_screen_names.shape))

#errors = pd.concat([missing_screen_names, missing_account_names, dupes], ignore_index=True)
#print('Size of combined \'errors\' data frame: ' + str(errors.shape))

#print(errors['error'].value_counts())

# re-do duplicate search while preserving the most recently added entries as non-dupes
# twitter_accts.sort_values('created_at', ascending = False, inplace = True)
# twitter_accts['is_duplicated'] = twitter_accts.duplicated(['screen_name'], keep = 'first')

# call platform APIs and get user IDs and other metadata for column of screen names

twitter_accts = accts[accts['service_key'] == 'twitter']
twitter_usernames = twitter_accts['username'].tolist()

facebook_accts = accts[accts['service_key'] == 'facebook']
facebook_urls = facebook_accts['url_from_username'].tolist()

'''
First time (or when you want to update):
    twitter_api = fetchTwitter(twitter_usernames)
Load from a prev saved file:
    twitter_api = loadTwitter()
'''
twitter_api = fetchTwitter(twitter_usernames)

'''
First time (or when you want to update):
    facebook_api = fetchFacebook(facebook_urls)
Load from a prev saved file:
    facebook_api = loadFacebook()
'''
facebook_api = fetchFacebook(facebook_urls)

twitter_api['last_posted_at'] = twitter_api['status'].apply(getLastTweet)
twitter_api[['last_posted_at', 'created_at']] = twitter_api[['last_posted_at', 'created_at']].apply(pd.to_datetime)
facebook_api['last_posted_at'] = facebook_api['feed'].apply(lambda x: getLastFacebookPost(x)).apply(pd.to_datetime)

twitter_api['last_posted_category'] = twitter_api['last_api_call'] - twitter_api['last_posted_at']
twitter_api['last_posted_category'] = twitter_api['last_posted_category'].apply(lastPostedCategory)

facebook_api['last_posted_category'] = facebook_api['last_api_call'] - facebook_api['last_posted_at']
facebook_api['last_posted_category'] = facebook_api['last_posted_category'].apply(lastPostedCategory)

twitter_api['screen_name_capitalized'] = twitter_api['screen_name']
twitter_api['screen_name'] = twitter_api['screen_name'].apply(str.lower)

# Merge the two datasets on the lower-case screen name field
twitter_merged = pd.merge(twitter_accts, twitter_api, how='outer', on=None, left_on='username', right_on='screen_name', left_index=False, right_index=False, sort=True, suffixes=('_usdr', '_api'), copy=False, indicator=False)

twitter_merged.to_pickle('data/twitter_merged.pkl')

facebook_merged = pd.merge(facebook_accts, facebook_api, how='outer', on=None, left_on='url_from_username', right_on='url', left_index=False, right_index=False, sort=True, suffixes=('_usdr', '_api'), copy=False, indicator=False)

facebook_merged.to_pickle('data/facebook_merged.pkl')

# run stats on results
print3col('','TWITTER','FACEBOOK')

total_usdr_records_twitter = twitter_merged['id_usdr'].nunique()
total_usdr_records_facebook = facebook_merged['id_usdr'].nunique()
print3col('Total USDR records:',total_usdr_records_twitter,total_usdr_records_facebook)

total_screen_names_twitter = twitter_merged['username'].nunique()
total_screen_names_facebook = facebook_merged['username_api'].nunique()
print3col('Unique usernames:',total_screen_names_twitter,total_screen_names_facebook)

total_twitter_ids = twitter_merged['id_api'].nunique()
total_facebook_ids = facebook_merged[facebook_merged['is_valid'] == True]['id_api'].nunique()
print3col('Accounts found using APIs:',total_twitter_ids,total_facebook_ids)
print3col('   % of unique screen names:',total_twitter_ids/total_screen_names_twitter,total_facebook_ids/total_screen_names_facebook)

# create dataframe of only the USDR entries with API results; only keep most recent entry if duplicate
twitter_merged_unique = twitter_merged.sort_values('created_at_usdr', ascending = False)
twitter_merged_unique.dropna(subset=['id_api'], inplace = True)
twitter_merged_unique.drop_duplicates(subset=['id_api'], keep = 'first', inplace = True)

facebook_merged_unique = facebook_merged.sort_values('created_at', ascending = False)
facebook_merged_unique.dropna(subset=['id_api'], inplace = True)
facebook_merged_unique.drop_duplicates(subset=['id_api'], keep = 'first', inplace = True)

verified_twitter = twitter_merged_unique['verified'].sum()
verified_facebook = facebook_merged_unique['is_verified'].sum()
print3col('Verified (with checkmark): ',verified_twitter,verified_facebook)
print3col('   % of accounts found in API:',verified_twitter/total_twitter_ids,verified_facebook/total_facebook_ids)

print3col('MOST RECENT POST BY CATEGORY','TWITTER','FACEBOOK')

dayago_twitter = (twitter_merged_unique['last_posted_category'] == 'within last 24 hours').sum()
dayago_facebook = (facebook_merged_unique['last_posted_category'] == 'within last 24 hours').sum()
print3col('Less than 24 hours ago:',dayago_twitter,dayago_facebook)
print3col('   % of accounts found in API:',dayago_twitter/total_twitter_ids,dayago_facebook/total_facebook_ids)

weekago_twitter = (twitter_merged_unique['last_posted_category'] == 'within last week').sum()
weekago_facebook = (facebook_merged_unique['last_posted_category'] == 'within last week').sum()
print3col('Within the last week:',weekago_twitter,weekago_facebook)
print3col('   % of accounts found in API:',weekago_twitter/total_twitter_ids,weekago_facebook/total_facebook_ids)

monthago_twitter = (twitter_merged_unique['last_posted_category'] == 'within last month').sum()
monthago_facebook = (facebook_merged_unique['last_posted_category'] == 'within last month').sum()
print3col('Within the last month:',monthago_twitter,monthago_facebook)

yearago_twitter = (twitter_merged_unique['last_posted_category'] == 'within last year').sum()
yearago_facebook = (facebook_merged_unique['last_posted_category'] == 'within last year').sum()
print3col('Within the last year:',yearago_twitter,yearago_facebook)

morethanayear_twitter = (twitter_merged_unique['last_posted_category'] == 'more than a year ago').sum()
morethanayear_facebook = (facebook_merged_unique['last_posted_category'] == 'more than a year ago').sum()
print3col('More than a year ago:',morethanayear_twitter,morethanayear_facebook)




