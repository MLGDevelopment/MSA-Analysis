import pandas as pd
from fredapi import Fred
import ast
import requests
import io
import os
import json




"""
IMPORT DB MODULES
"""


# TODO: FIX THIS BULLSHIT

# fetch_CS_price_index()
# census_conn = Census(CENSUS_API)
# num_homes = census_conn.acs5.get(('NAME', 'B25034_010E'), {'for': 'state:{}'.format(states.MD.fips)})


FRED_API = "7b01ffec8a8b5fa92a16c5af3f72ff14"
WALK_SCORE = "655a7c936568b9030f821b9c549ee261"

# Load all CBSAs from external file

fred = Fred(api_key=FRED_API)


class BureauEconomicAnalysis:

    API_KEY = "06800A25-B03C-45A8-885B-BB2D337FA783"

    def __init__(self):
        self.call = "https://apps.bea.gov/api/data/?UserID={API_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP2&LineCode=2&Year=ALL&GeoFips=CSA&ResultFormat=JSON".format(API_KEY=self.API_KEY)
        data = requests.get(self.call).content
        json.loads(data)
        pd.read_json(data)
        # io.BytesIO(data.decode('utf-8'))
        df = pd.read_csv(io.StringIO(data.decode('latin-1')), converters={'CBSA': str})
        df["CBSA"] = df["CBSA"].map(int)


class Mappings:

    def __init__(self):

        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "mappings")
        self.states_df = pd.read_excel(os.path.join(self.path, "States.xlsx"), dtype={"fips": str})
        self.states_df.set_index("fips", inplace=True)
        # self.cbsa_fips_df = pd.read_excel(os.path.join(self.path, "county-msa-csa-crosswalk.xlsx"),
        #                                   dtype={"County Code": str,
        #                                          "MSA Code": str,
        #                                          "CSA Code": str,
        #                                          "CBSA Id": str})

        self.cbsa_fips_df = pd.read_excel(os.path.join(self.path, "cbsa_fips_mapping_master.xlsx"),
                                          dtype={"CBSA Code": str,
                                                 "Metropolitan Division Code": str,
                                                 "Combined County FIPS": str,
                                                 })

        # LOAD ALL STATES
        self.cbsa_fips_df.set_index("Combined County FIPS", drop=False, inplace=True)
        # self.cbsa_fips_df["State ID"] = [i[:2] for i in self.cbsa_fips_df["County Id"].values.tolist()]
        self.cbsa_fips_df["State Name"] = self.cbsa_fips_df["FIPS State Code"].map(self.states_df["state"])
        self.cbsa_fips_df["State"] = self.cbsa_fips_df["FIPS State Code"].map(self.states_df["abbreviation"])


def parse_string_list(data):
    """
    CONVERTS A STRING REPRESENTED LIST TO AN ACTUAL LIST
    :return:
    """
    txt = data.decode()
    x = ast.literal_eval(txt)
    return x


class CensusBureauPermits:

    def __init__(self):
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.datasets_path = os.path.join(self.path, "data", "datasets", "cb_permit_data.xlsx")
        self.permit_data = pd.read_excel(self.datasets_path)





def fetch_CS_price_index():
    cs_mappings = {38060: "PHXRSA",
                   31080: "LXXRSA",
                   41740: "SDXRSA",
                   41860: "SFXRSA",
                   19740: "DNXRSA",
                   47900: "WDXRSA",
                   33100: "MIXRSA",
                   45300: "TPXRSA",
                   12060: "ATXRSA",
                   16980: "CHXRSA",
                   14460: "BOXRSA",
                   19820: "DEXRSA",
                   33460: "MNXRSA",
                   16740: "CRXRSA",
                   29820: "LVXRSA",
                   35620: "NYXRSA",
                   17420: "CEXRSA",
                   38860: "POXRSA",
                   19100: "DAXRSA",
                   42660: "SEXRSA"}

    mapping = {}
    for k, v in cs_mappings.items():
        type = "json"
        cbsa = k
        sid = v
        # path = "https://api.stlouisfed.org/fred/series?series_id={SERIES_ID}&api_key={API_KEY}&file_type={TYPE}".format(SERIES_ID=sid, API_KEY=FRED_API, TYPE=type)

        series = fred.get_series(sid)
        # series.index = pd.to_datetime(series.index, unit='D')
        mapping[cbsa] = series

    # find longest series to build index
    _max = 0
    id = 0
    for k, v in mapping.items():
        t_max = max(_max, len(mapping[k]))
        if t_max > _max:
            id = k
            _max = t_max

    index = mapping[id].index

    df = pd.DataFrame(index=index)

    for k, v in mapping.items():
        df[k] = df.index.map(mapping[k])





def main():
    pass
    cbp = CensusBureauPermits()


if __name__ == "__main__":
    main()

