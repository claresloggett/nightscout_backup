
'''
Back up all nightscout info to CSV files using the web API. 
BGL entries will be saved to nightscout_entries.csv.gz .
Treatments (carbs, insulin, profile changes etc) will be saved to 
multiple files nightscout_treatments_<treatmenttype>.csv.gz .
'''

# This is a simple implementation with some limitations:
# - Dataframes are built and then written out; all data must fit in memory
# - Server URL and other parameters are hard-coded - edit them below
# - Backs up all data; can't specify date ranges or add incrementally to
#   existing backups
# - Any preexisting backup files are overwritten

# base_url must be replaced with the URL of your nightscout server
base_url = ""
batchsize = 2000
max_records = None  # don't retrieve more than this, in total


import requests
import pandas as pd
import sys
from collections import defaultdict


def get_entries(api_endpoint='entries', datefield='dateString'):
    '''
    Get all BGL entries and return as a dataframe.
    '''
    requeststring = f"{base_url}api/v1/{api_endpoint}.json?count={batchsize}"
    print(requeststring)
    first_response = requests.get(requeststring)
    data = pd.DataFrame(first_response.json())
    if len(data)==0:
        print("No data found")
        sys.exit(1)
    print("Retrieved {} records {} - {}".format(
        len(data), data[datefield].iloc[-1], data[datefield].iloc[0]
    ))

    num_records = len(data)
    all_data = [data]

    while True:
        earliest_datestr = data[datefield].iloc[-1]
        # where?find[dateString][$gte]=2016-09&find[dateString][$lte]=2016-10&find[sgv]=100`
        # should work as it's a datatime not just a date - there should only be an overlap of 1 at most
        requeststring = f"{base_url}api/v1/{api_endpoint}.json?count={batchsize}"
        requeststring += f"&find[{datefield}][$lt]={earliest_datestr}"
        print(requeststring)
        response = requests.get(requeststring)
        data = pd.DataFrame(response.json())
        #print(data.columns)
        if len(data)==0:
            break
        print("Retrieved {} records {} - {}".format(
            len(data), data[datefield].iloc[-1], data[datefield].iloc[0]
        ))
        all_data.append(data)
        num_records += len(data)
        if max_records is not None and num_records >= max_records:
            print("Max records exceeded, stopping")
            break

    data = pd.concat(all_data).drop_duplicates()

    print(data.shape)
    print(data.columns)

    return data

def split_data(data):
    """
    Given a json list of events, split on event type and return a dict of
    DataFrames where the keys are event types.
    Treat the "Bolus Wizard" event type differently: extract the boluscalc
    field and parse dict into separate columns.
    """
    result = dict()
    eventtypes_present = set([event['eventType'] for event in data])
    for et in eventtypes_present:
        events = [event for event in data if event['eventType']==et]
        if et=="Bolus Wizard":
            result[et] = pd.DataFrame(events).drop('boluscalc', axis=1)
            bc = pd.DataFrame([e['boluscalc'] for e in events if 'boluscalc' in e])
            bc_exists = ['boluscalc' in e for e in events]
            for field in bc:
                result[et].loc[bc_exists, 'boluscalc_'+field] = bc[field]
        else:
            result[et] = pd.DataFrame(events)
    return result

'''
# Currently unused
def unpack_bolus_wizard(df):
    """
    Given the dataframe obtained by downloading events of eventType=="Bolus Wizard",
    unpack the boluscalc field json into separate fields.
    """
    boluscalcfield = 'boluscalc'
    if boluscalcfield not in df.columns:
        raise ValueError("{} not found when trying to unpack bolus wizard data".format(boluscalcfield))
    boluscalc_df = pd.DataFrame(list(df[boluscalcfield]))
    print(df.shape, boluscalc_df.shape)
    return pd.concat([df, boluscalc_df], axis=1)
'''

def get_treatments(api_endpoint='treatments', datefield='created_at'):
    '''
    Get all treatments and return as a dict of dataframes, where keys 
    are treatment types and values are corresponding dataframes.
    Each dataframe only holds columns for the fields that occur in that
    treatment type.
    '''
    requeststring = f"{base_url}api/v1/{api_endpoint}.json?count={batchsize}"
    print(requeststring)
    first_response = requests.get(requeststring)
    # In this case data is json, not dataframe
    data = first_response.json()
    latest_datestr = data[0][datefield]
    earliest_datestr = data[-1][datefield]
    if len(data)==0:
        print("No data found")
        sys.exit(1)
    print("Retrieved {} records {} - {}".format(
        len(data), earliest_datestr, latest_datestr
    ))

    # some records don't have eventType; ignore these
    # seem to be empty records created by Spike
    data = [e for e in data if 'eventType' in e.keys()]

    num_records = len(data)
    all_data = defaultdict(list)
    for eventtype, df in split_data(data).items():
        all_data[eventtype].append(df)

    while True:
        earliest_datestr = data[-1][datefield]
        # where?find[dateString][$gte]=2016-09&find[dateString][$lte]=2016-10&find[sgv]=100`
        # should work as it's a datatime not just a date - there should only be an overlap of 1 at most
        requeststring = f"{base_url}api/v1/{api_endpoint}.json?count={batchsize}"
        requeststring += f"&find[{datefield}][$lt]={earliest_datestr}"
        print(requeststring)
        response = requests.get(requeststring)
        data = response.json()
        if len(data)==0:
            break
        print("Retrieved {} records {} - {}".format(
            len(data), earliest_datestr, data[0][datefield]
        ))
        data = [e for e in data if 'eventType' in e.keys()]
        for eventtype, df in split_data(data).items():
            all_data[eventtype].append(df)
        num_records += len(data)
        if max_records > 0 and num_records >= max_records:
            print("Max records exceeded, stopping")
            break


    dataframes = {eventtype:pd.concat(dflist)  #.drop_duplicates()
                for eventtype, dflist in all_data.items()}
    # TODO: check for duplicates following unpacking
    return dataframes

def main():
    print("Retrieving BGL entries")
    entries = get_entries()
    # Infering gzip from filename does not appear to work
    print("Saving entries")
    entries.to_csv('nightscout_entries.csv.gz', index=False, compression='gzip')

    print("Retrieving treatments")
    treatments = get_treatments()
    for eventtype, df in treatments.items():
        print(f"Saving {eventtype}")
        df.to_csv(f'nightscout_treatments_{eventtype}.csv.gz', index=False, compression='gzip')


if __name__=="__main__":
    main()