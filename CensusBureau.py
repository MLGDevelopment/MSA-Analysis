import pandas as pd
from urllib.request import urlopen
import requests
import io
import os
import ast
import numpy as np

# datasets in /data https://api.census.gov/data.html
# see: https://www.census.gov/data/developers/data-sets/acs-1year/notes-on-acs-api-variable-formats.html

# ACS SUBJECT VARIABLES: https://api.census.gov/data/2019/acs/acs5/subject/variables.html
#

# ASC VARIABLES: https://api.census.gov/data/2019/acs/acs5/variables.html
# https://www.census.gov/programs-surveys/acs/guidance/estimates.html

# WHEN TO USE ACS 1, 3, 5
# https://www.census.gov/programs-surveys/acs/guidance/estimates.html

# CB Delineation files
# https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html

acs_acs5_subject = "https://api.census.gov/data/{}/acs/acs5/subject/"

# todo: check comments below

ACS_IDS = {"Total Population": "B01003_001E",
           "Total Households": "S1101_C01_001E",
           "Average Household Size": "S1101_C01_002E",
           # "MEDIAN PER CAPITA INCOME - PAST 12 MONTHS": "B06011", // was this from acs5? or 1?
           # "MEDIAN HOUSEHOLD INCOME - PAST 12 MONTHS": "B19013",
           # "Median Household Size": "S1101_C01_002E",
           # "Total Households": "B11016",
           }


def parse_string_list(data):
    """
    CONVERTS A STRING REPRESENTED LIST TO AN ACTUAL LIST
    :return:
    """
    txt = data.decode()
    x = ast.literal_eval(txt)
    return x


class CensusBureau:
    API_KEY = "92227c1ecf7ca9a88fd6069ff8c8107bc45f842a"
    base_path = "https://api.census.gov/data/{YEAR}/{REPORT}?get={NAME}&for={GEO}&key={API_KEY}"

    # identifies
    all_states = "us:1"
    all_regions = "region:*"
    all_division = "division:*"
    all_counties = "county:*"
    all_county_subdivisions = "county%20subdivision:*"

    def __init__(self):
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        self.datasets = os.path.join(self.curr_dir, "data", "datasets", "census bureau")
        self.report_features_path = os.path.join(self.curr_dir, "data", "mappings", "CensusReportList.xlsx")
        self.pop_features = open(os.path.join(self.curr_dir, "pop_features.txt")).read().split("\n")
        self.report_features = pd.read_excel(self.report_features_path )
        self.cb_monthly_permits = self.load_permit_dataset()

    def load_population_data(self):
        fname = "2010-2019 Detailed Population Estimates.xlsx"
        path = os.path.join(self.datasets, fname)
        self.population_data_2010_2019 = pd.read_excel(path, converters={'COMBINED_COUNTY_ID': str})
        return self.population_data_2010_2019

    def load_permit_dataset(self):
        self.monthly_permits_path = os.path.join(self.datasets, "monthly permits.xlsx")
        self.monthly_permits = pd.read_excel(self.monthly_permits_path, converters={"CBSA": str})
        return self.monthly_permits

    def fetch(self, path):
        with requests.get(path) as url:
            res = url.json()
        return res

    def fetch_population_data(self):
        """
        EXAMPLE:
        https://api.census.gov/data/2019/pep/population?
        get=POP&
        for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&
        key=YOUR_KEY_GOES_HERE
        :return:
        """

        _base = "https://api.census.gov/data/2019/pep/population?get={GET}&for={FOR}&in={IN}&key={KEY}"
        _base = "https://api.census.gov/data/2019/pep/population?get={GET}&for={FOR}&key={KEY}"
        _get = "POP"
        _for = "metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
        path = _base.format(GET=_get, FOR=_for, KEY=self.API_KEY)

    def fetch_county_population_csv(self, export=False):
        """
        MSA Link = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv"

        NOTE: Using this because cannot find data via API

        :return:
        """

        path_counties = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv"
        data = requests.get(path_counties).content

        df = pd.read_csv(io.StringIO(data.decode('latin-1')), converters={'STATE': str, 'COUNTY': str})
        df.set_index(df.columns[:7].values.tolist(), drop=True, inplace=True)

        del df["CENSUS2010POP"]
        del df["ESTIMATESBASE2010"]

        df = df.stack()
        df = df.reset_index(drop=False)

        for i, row in df.iterrows():
            df.set_value(i, 'year', int(row['level_7'][-4:]))
            df.set_value(i, 'level_7', row['level_7'][:-4])

        df["COMBINED_COUNTY_ID"] = df["STATE"] + df["COUNTY"]
        df.rename(columns={0: 'estimate'}, inplace=True)
        df.rename(columns={'level_7': 'values'}, inplace=True)

        if export:
            path = os.path.join(self.datasets, "2010-2019 Detailed Population Estimates - Counties.xlsx")
            df.to_excel(path)
        return df


    def fetch_msa_population_csv(self, export=False):
        """
        MSA Link = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv"

        NOTE: Using this because cannot find data via API

        :return:
        """

        path_msa = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv"
        data = requests.get(path_msa).content

        df = pd.read_csv(io.StringIO(data.decode('latin-1')), converters={'STATE': str, 'COUNTY': str})
        df.set_index(df.columns[:5].values.tolist(), drop=True, inplace=True)

        del df["CENSUS2010POP"]
        del df["ESTIMATESBASE2010"]

        df = df.stack()
        df = df.reset_index(drop=False)

        for i, row in df.iterrows():
            df.set_value(i, 'year', int(row['level_5'][-4:]))
            df.set_value(i, 'level_5', row['level_5'][:-4])

        df.rename(columns={0: 'estimate'}, inplace=True)
        df.rename(columns={'level_5': 'values'}, inplace=True)

        if export:
            path = os.path.join(self.datasets, "2010-2019 Detailed Population Estimates - MSA.xlsx")
            df.to_excel(path)
        return df

    def fetch_acs_survey_data(self, year, report, study_name, geo, api_key, study_description=""):
        path = self.base_path.format(YEAR=year, REPORT=report, NAME="NAME,"+study_name, GEO=geo, API_KEY=api_key)
        res = self.fetch(path)

        convert_col_index = -1
        for i, val in enumerate(res[0]):
            if val == study_name:
                if study_description:
                    res[0][i] = study_description
                convert_col_index = i
        df = pd.DataFrame(res)
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])
        df["year"] = year
        df["combined_county_id"] = df["state"] + df["county"]
        df["combined_county_id"] = df["combined_county_id"].astype(str)
        df.iloc[:, convert_col_index] = df.iloc[:, convert_col_index].astype(float)
        df["year"] = df['year']
        df = df.set_index(["combined_county_id", "year"])
        return df

    def build_acs_report(self, years=[], export=False):
        census = CensusBureau()

        if not years:
            years = list(range(2010, 2020))
            years.reverse()

        master = pd.DataFrame()
        for i, row in self.report_features.iterrows():
            temp_df = pd.DataFrame()
            for year in years:
                df = census.fetch_acs_survey_data(year=year,
                                                  report=row["report"],
                                                  study_name=row["id"],
                                                  geo=census.all_counties,
                                                  api_key=census.API_KEY,
                                                  study_description=row["name"])
                temp_df = temp_df.append(df)
            if master.empty:
                master = temp_df
            else:
                master = pd.concat([master, temp_df[row["name"]]], axis=1)

        self.cb_report = master
        self.cb_report.reset_index(drop=False, inplace=True)

        if export:
            self.cb_report.to_excel("census_bureau_report.xlsx")
        return self.cb_report

    def get_permit_trend(self, cbsa_id, lookback):
        cbsa_permits = self.cb_monthly_permits[self.cb_monthly_permits['CBSA'] == cbsa_id]
        cbsa_permits.sort_values('period', inplace=True)
        cbsa_permits_last_3_years_average = cbsa_permits.tail(lookback)['5 Units or More'].mean() * 12
        cbsa_permits_historical_average = cbsa_permits.tail(len(cbsa_permits) - lookback)['5 Units or More'].mean() * 12
        return cbsa_permits_last_3_years_average, cbsa_permits_historical_average


if __name__ == "__main__":
    cb = CensusBureau()
    cb.fetch_msa_population_csv(export=1)

    #cb.get_permit_trend('28940', 36)
    #df = cb.fetch_county_population_csv(export=True)

    # cb.build_acs_report()

    print
    df.to_excel('2010-2019 Population Detail.xlsx')

# master = pd.DataFrame()
# for year in years:
#     df = census.fetch_acs_survey_data(year, study_name=ACS_IDS["Total Households"],
#                                       geo=census.all_counties,
#                                       api_key=census.API_KEY,
#                                       study_description="Total Households")
#
#     df["combined_county_id"] = df["state"] + df["county"]
#     master = master.append(df)
#
# master.to_excel("total_households.xlsx")


# master = pd.DataFrame()
# for year in years:
#     df = census.fetch_acs_survey_data(year, study_name=ACS_IDS["MEDIAN PER CAPITA INCOME - PAST 12 MONTHS"],
#                                       geo=census.all_counties,
#                                       api_key=census.API_KEY, study_description="Median Per Capita Income")
#     df["combined_county_id"] = df["state"] + df["county"]
#     master = master.append(df)
#
# master.to_excel("median_per_capita_income.xlsx")
#
# master = pd.DataFrame()
# for year in years:
#     df = census.fetch_acs_survey_data(year, study_name=ACS_IDS["MEDIAN HOUSEHOLD INCOME - PAST 12 MONTHS"],
#                                       geo=census.all_counties,
#                                       api_key=census.API_KEY, study_description="Median Household Income")
#     df["combined_county_id"] = df["state"] + df["county"]
#     master = master.append(df)
#
# master.to_excel("median_hhi.xlsx")