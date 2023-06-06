import rollbar
import collections
rollbar.init('bb5f317870614838b7493552dee6f639')
from pymongo import MongoClient

FB_VERSION = 'fb_version'
FB_URL = 'fb_url'
FACEBOOK_ACCESS_USER_ID = 'facebook_access_user_id'
FACEBOOK_ACCESS_TOKEN = 'facebook_access_token'
FACEBOOK_BUSINESS_ID = 'facebook_business_id'
FACEBOOK_APP_SECRET_KEY = 'facebook_app_secret_key'
FACEBOOK_XPLANCK_ADYOGI_APP_ID = 'facebook_xplanck_adyogi_app_id'
PARSE_APPLICATION_ID = 'parse_application_id'
PARSE_REST_API_KEY = 'parse_rest_api_key'
PARSE_SESSION_TOKEN = 'parse_session_token'

def getConstants():
    client = MongoClient(
        'mongodb://admin:4MZ1UUe3nFaej4RzUu1RR6jn@mongodb4.back4app.com:27017/0b55c9eb2c2d4d20b572bf2a3b29f834?ssl=true')
    # getting database reference by database name.
    db = client['0b55c9eb2c2d4d20b572bf2a3b29f834']
    # table from which we have to fetch data i.e "Constants" from parse.
    collection = db["Constants"]
    documents = collection.find()
    map = {}

    if documents.count() > 0:
    # iterating through documents one by one and saving to bigQuery.
        for aDocument in documents:
             map[aDocument['key']] = aDocument['value']

    return map
