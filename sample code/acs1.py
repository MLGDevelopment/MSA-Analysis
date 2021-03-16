import json
import pandas as pd
import numpy as np
import re
import requests



####
# The Json file...stored in ./data/get_acs1.json
get_acs1_json = {
  "area":"place:*&in=state:*",
  "vintages":[
    2017,
    2010
  ],
  "table_name" : "acs1_place",
  "names":[
    "NAME",
    "CBSA",
    "CSA",
    "SUMLEVEL",
    "UA",
    "GEO_ID",
    "METDIV",
    "LSAD_NAME",
    "DIVISION",
    "REGION",
    "COUNTY",
    "STATE"
  ],
  "att":{
    "B01003_001E":{
      "name":"Total_Population"
    },
    "B25077_001E":{
      "name":"Median_Home Value"
    },
    "B25105_001E":{
      "name":"Median_Monthly_Housing_Costs"
    },
    "B25092_001E":{
      "name":"Median_Selected_Monthly_Owner_Costs_as_a_Percentage_of_Household_Income"
    },
    "B19013_001E":{
      "name":"Median_Household_Income"
    },
    "B19301_001E":{
      "name":"Per_Capita_Income"
    },
    "B01002_001E":{
      "name":"Median_Age"
    },
    "B25012_004E":{
      "name":"Total_Housing_Units_with_Children_under_18"
    },
    "B25013_006E":{
      "name":"Tenure_by_Educational_Attainment_of_Householder_(Bachelors_Degree_or_Higher)"
    },
    "B00002_001E":{
      "name":"Total_Number_of_Housing_Units_(Housing_Density)"
    },
    "B11001_002E":{
      "name":"Household_Type_Family_Rate",
      "calc":[
        "B11001_002E",
        "B11001_001E",
        "divide"
      ]
    },
    "B13008_002E":{
      "name":"Women_Who_had_Birth_within_Yr_Rate",
      "calc":[
        "B13008_002E",
        "B13008_001E",
        "divide"
      ]
    },
    "B02001_002E":{
      "name":"White_Race_Rate",
      "calc":[
        "B02001_002E",
        "B02001_001E",
        "divide"
      ]
    },
    "B17001_002E":{
      "name":"Poverty_Rate",
      "calc":[
        "B17001_002E",
        "B17001_001E",
        "divide"
      ]
    },
    "B99072_007E":{
      "name":"Same_House_for_over_year_Rate",
      "calc":[
        "B99072_007E",
        "B99072_001E",
        "divide"
      ]
    },
    "B25013_011E":{
      "name":"Living_in_Same_House_Rate",
      "calc":[
        "B25013_011E",
        "B25013_001E",
        "divide"
      ]
    },
    "C24050_015E":{
      "name":"Management_Sciences_White_Collar_Rate",
      "calc":[
        "C24050_015E",
        "C24050_001E",
        "divide"
      ]
    }
  }
}


def load_json_file(file_path):
    with open(file_path) as json_file:
        json_data = json.load(json_file)
        return json_data


def get_api_keys(file='./data/api_key.json'):
    api_keys = json.load(open(file))
    # PB_API_KEY = keys['pushbullet-api']
    # CRAWLERA_API_KEY = keys['crawlera-api']['key-us-only']
    # GOOGLE_API_KEY = keys['google-api']
    # GEO_NAMES_API_KEY = keys['geonames-api']
    # CENSUS_API_KEY = keys['census-api']
    return api_keys


def format_att_get_string(json_data):

    # NAME, GEOID, LSAD_NAME, and etc
    names = ','.join([name for name in json_data['names']])

    # Table Attributes like Mean Income and etc (B19013_001E)
    att = ','.join([att for att in json_data['att']])

    # Pulling the calculation field's denominator (like Whites/TotalPopulation)
    # These cols will dropped after calculation is made
    calcs = ','.join([
        calc_vals['calc'][1]
        for att, calc_vals in json_data['att'].items()
        if 'calc' in calc_vals
    ])

    return '{0},{1},{2}'.format(names,att,calcs)


def get_acs1_vars(year, get, area, CENSUS_API_KEY, missing=[]):
    """ Builds the url for ACS1 json query based on chosen attributes and year
        Removes missing Table Attributes if they exist...to avoid a json error
    """
    url = 'https://api.census.gov/data/{0}/acs/acs1?get={1}&for={2}&key={3}'
    req_url = url.format(year, get, area, CENSUS_API_KEY)
    r = requests.get(req_url)

    if 'error' in r.text:

        # Find all the missing attributes...that don't exist for this acs1 vintage
        missing_attribute = re.findall(
            r'[A-Z0-9]{6}_[A-Z0-9]{3,4}', r.text)[0]  # B25013_001E (only returns one :\)
        group = missing_attribute.split('_')[0]  # B25013
        atts_in_missing_grp = [att for att in get if group in att]

        # Store all the attributes (in this group)
        for att in atts_in_missing_grp:
            missing.append(att)

        # Take the missing  attributes out of the get array
        # Query the ACS1 again...see if JSON is successful
        newget = [att.strip() for att in get if group not in att]
        return get_acs1_vars(year, newget, area, CENSUS_API_KEY,
                             missing)

    # All missing table attributes removed, json string successful
    # Return that json string and all the missing attributes (Mean income)
    # Add those missing attributes back as column with a none value
    else:
        return missing, r.json()


def json2df(json_data):
    df = pd.DataFrame(json_data)
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    return df


def column_year_suffix(col, year):
    if re.findall(r'_[0-9]{3,4}E', col):
        return '{0}_{1}'.format(col, year)
    else:
        return col


def add_calc_cols(calc_atts, df):

    # Iterate through Calc Columns add Result as a New Pandas Col
    for att, row in calc_atts.items():
        col = '{0}_{1}'.format(att, 'CALC')
        numerator_col = row['calc'][0]
        denom_col = row['calc'][1]
        operation = row['calc'][2]

        if all([numerator_col, denom_col, operation]):
            if operation == 'divide':

                def divide(row, numerator_col, denom_col):
                    try:
                        num_val = row[numerator_col]
                        denom_val = row[denom_col]
                        return num_val / denom_val
                    except Exception as e:
                        return np.nan

                df[col] = df.apply(lambda row: divide(row, numerator_col, denom_col), axis=1)
                df.loc[~np.isfinite(df[col]), col] = np.nan

            else:
                raise Exception('Error: Operation {} was not divide--as expected'.format(operation))
        else:
            raise Exception('Error: Calculated Table Variables Missing')

    return df


def get_censusr_col_name(col_name):
    """get_acs_col_name('B01003_001E', '2017')
       Gets name from Census Reporter
    """

    # B13008_002E and such
    table_name = re.findall(r'([\w]{6,7}_[\w+]{4})', col_name)
    if len(table_name) != 0:
        table_name = table_name[0]

    # Skip NAME, STATE, COUNTY, LSAD_NAME, GEO_ID and etc
    if not re.findall(r'_[0-9]{3,4}E', col_name):
        return col_name  # Return original name...like GEO_ID

    # B25077_001E_2016 or B25077_001E_DELTA
    else:
        try:
            # If column name has no year, set it to DELTA
            year = re.findall(r'(_[\d]{4})', col_name)
            if len(year) == 0:
                year = 'DELTA'  # Covers multiple years (2017 and 2010)
            else:
                year = year[0]

            group = col_name.split('_')[0]

            cr_api = 'https://api.censusreporter.org/1.0/table/{}'
            r = requests.get(cr_api.format(group))
            data = r.json()
            table_id = data['table_id'].strip()
            universe = data['universe'].strip()
            table_title = data['table_title'].strip().replace('(Dollars)', '')
            if table_title == universe:
                table_title = ''

            return '{0}, {1}({2})'.format(universe, table_title, col_name)

        except Exception as e:
            print('Attribute {0} Group not found in Census Reporter: {1}'.format(col_name, e))

def censusr2codes(col_name):
    # Not written yet...
    return

####
# Main ASC1 Function
def acs1_deltas_calcs():
    # Load Json File for that Outlines Table Attributes
    API_KEY = "92227c1ecf7ca9a88fd6069ff8c8107bc45f842a"
    query_json = get_acs1_json
    CENSUS_API_KEY = API_KEY

    # Format the Table Attributes of JSON into a URL string
    get = format_att_get_string(query_json)  # B13008_002E, B13008_001E, ...
    area = query_json['area']  # 'place:*&in=state:*'
    years = query_json['vintages'] # [2016, 2010]
    mdf = pd.DataFrame([])

    # Filter for Table attributes that are calculated (White/TotalPop)
    # Example: "B13008_002E":{ "calc":["B13008_002E","B13008_001E","divide"]}
    calc_atts = ({
            att: row
            for att, row in query_json['att'].items()
            if 'calc' in row
        })

    # Add the Attribute tables that are numbers (not NAME or GEO_ID)
    # Main Table Attributes (Latino)
    numbers_att = [k for k, v in query_json['att'].items()]

    # These Table Attributes are dropped after the calculation is made (like Total)
    numbers_att_calc_to_drop_later = ([
            row['calc'][1]  # Usually a 'Total' Value
            for att, row in query_json['att'].items()
            if 'calc' in row
        ])

    # Delete any duplicates just in case I screwed up
    numbers_att = list(set(numbers_att + numbers_att_calc_to_drop_later))

    # Names (like GEO_ID, and LSAD_NAME)
    names_att = query_json['names']

    for year in years:
        # Json Returned from Census ACS1 Query and any missing Table Attributes
        missing_attributes, result_json = get_acs1_vars(year, get, area, CENSUS_API_KEY)

        # Convert json to dataframe
        df = json2df(result_json)

        # Coerce number columns to integer (skip cols like NAME, DATE, and etc)
        for col in numbers_att:
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')

        # ADD Calculated Attributes (like White/Total Population)
        df = add_calc_cols(calc_atts, df)

        # Drop the Original (returned json) Calculation Columns (Like Total)
        df.drop(numbers_att_calc_to_drop_later, axis=1, inplace=True)

        # Add a Year Suffix to the Column Header (Can Calculate Year Deltas)
        df.rename(columns=lambda x: column_year_suffix(x, year), inplace=True)

        # Merge columns from both vintages(2017,2010) into one dataframe
        mdf = pd.concat([mdf, df], axis=1)

    # Delete Duplicate Columns shared by vintages (NAME, GEO_ID, and etc)
    mdf = mdf.loc[:, ~mdf.columns.duplicated()]

    ######
    # DELTAS (2017-2010) / 2010
    for att in numbers_att:
        end_col = '{0}_{1}'.format(att, max(years))
        start_col = '{0}_{1}'.format(att, min(years))
        delta_col = '{0}_{1}'.format(att, 'DELTA')

        def deltas(row, end_col, start_col):
            try:
                delta = (row[end_col]-row[start_col]) / row[start_col]
                return delta
            except Exception as e:
                return np.nan

        mdf[delta_col] = mdf.apply(lambda row: deltas(row, end_col, start_col), axis=1)

    #####
    # Replace Crappy Column Names with Census Reporter
    mdf.rename(columns=lambda col:  get_censusr_col_name(col), inplace=True)

    table_name = query_json['table_name']

    return mdf


if __name__ == '__main__':
    acs1_deltas_calcs()
    # print(get_censusr_col_names('B25077_001E_2016'))
    # print(get_censusr_col_names('B25077_001E_DELTA'))

