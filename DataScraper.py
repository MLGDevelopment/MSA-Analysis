from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
from census import Census
from us import states
from urllib.request import urlopen
from fredapi import Fred
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import ast
from urllib.request import urlopen
import requests
import io
import os
import json
from db_builder import session, RentComp, AxioProperty
from datetime import date
from sqlalchemy.sql import exists



# fetch_CS_price_index()
# census_conn = Census(CENSUS_API)
# num_homes = census_conn.acs5.get(('NAME', 'B25034_010E'), {'for': 'state:{}'.format(states.MD.fips)})


FRED_API = "7b01ffec8a8b5fa92a16c5af3f72ff14"
WALK_SCORE = "655a7c936568b9030f821b9c549ee261"

# Load all CBSAs from external file
CBSA = pd.read_excel("CBSA_Master.xls")
fred = Fred(api_key=FRED_API)
CBSA_consolidated_mapping = pd.pivot_table(CBSA, index=["CBSA Title"])["CBSA Code"]


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


class CensusBureau:

    API_KEY = "92227c1ecf7ca9a88fd6069ff8c8107bc45f842a"

    def __init__(self):
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.stored_data = os.path.join(self.path, "data", "stored_data")
        self.pop_features = open(os.path.join(self.path, "pop_features.txt")).read().split("\n")

    def fetch(self, path):
        with urlopen(path) as url:
            res = url.read()
            p_res = parse_string_list(res)
        return p_res

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

    def fetch_population_csv(self):
        path = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv"
        data = requests.get(path).content
        # io.BytesIO(data.decode('utf-8'))
        df = pd.read_csv(io.StringIO(data.decode('latin-1')), converters={'CBSA': str})
        df["CBSA"] = df["CBSA"].map(int)
        return df


class Mappings:

    def __init__(self):

        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "mappings")
        # ZCTA TO CBSA NOTE: MANY TO MANY RELATIONSHIP
        # self.zcta_cbsa_df = pd.read_excel(os.path.join(self.path, "ZCTA_CBSA.xlsx"), converters={'ZCTA5': str})

        # ZIP CODE TO COUNTY MAPPING (WITH FIPS)
        # self.zcta_county_df = pd.read_excel(os.path.join(self.path, "ZCTA_County.xlsx"), converters={'ZCTA5': str})

        # ALL ZIPS CODES
        # self.zip_codes = pd.read_excel(os.path.join(self.path, "ZIP Codes.xlsx"))

        # CBSA TO STATES MAPPING
        self.cbsa_fips_df = pd.read_excel(os.path.join(self.path, "CBSA_FIPS.xlsx"))

        # LOAD ALL STATES
        self.states_df = pd.read_excel(os.path.join(self.path, "States.xlsx"))
        self.states_df.set_index("fips", inplace=True)


def parse_string_list(data):
    """
    CONVERTS A STRING REPRESENTED LIST TO AN ACTUAL LIST
    :return:
    """
    txt = data.decode()
    x = ast.literal_eval(txt)
    return x


# def some_data_fetch():
#     path = "https://api.census.gov/data/2014/pep/natstprc?get=STNAME,POP&DATE_=7&for=state:*&key={API_KEY}".format(
#         API_KEY=CENSUS_API)
#     with urlopen(path) as url:
#         res = url.read()
#         p_res = parse_string_list(res)
#     return p_res
#
#
# def fetch_cbsa_population():
#     path = "https://api.census.gov/data/2019/pep/population?get=POP&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={API_KEY}".format(API_KEY=CENSUS_API)
#
#     with urlopen(path) as url:
#         res = url.read()
#         p_res = parse_string_list(res)
#
#     mapping = {}
#     for i in range(1, len(p_res)):
#         _cbsa = p_res[i]
#         population = _cbsa[0]
#         cbsa = _cbsa[1]
#         mapping[int(cbsa)] = int(population)
#
#     CBSA["Population"] = CBSA["CBSA Code"].map(mapping)


class Axio():

    def __init__(self):
        self.chrome_options = Options()
        #self.chrome_options.add_argument("--headless")
        self.login_path = "https://axio.realpage.com/Home"
        self.market_trends_path = "https://axio.realpage.com/Report/MarketTrendSearch"
        self.property_report_path = "https://axio.realpage.com/PropertyReport/UnitMix/"
        self.logged_in = False

        try:
            self.driver = webdriver.Chrome(executable_path="chromedriver.exe", chrome_options=self.chrome_options)
        except:
            self.driver = webdriver.Chrome(executable_path="chromedriver.exe", chrome_options=self.chrome_options)

        self.base = ""

    def login(self, username, password, login_link):
        """
        METHOD TO LOG INTO YARDI
        :param username:
        :param password:
        :param login_link:
        :return:
        """
        try:
            self.driver.get(login_link)
            username_field = self.driver.find_element_by_id("username")
            username_field.clear()
            username_field.send_keys(username)
            self.driver.find_element_by_id("password").click()
            self.driver.find_element_by_id("password").send_keys(password)
            self.driver.find_element_by_id("btnSignIn").click()
            self.logged_in = True
            time.sleep(7)
            return 1
        except:
            return 0

    def mlg_axio_login(self):
        mlg_username = "nburmeister@mlgcapital.com"
        mlg_password = "nikMLGC9ejfq7f"
        return self.login(mlg_username, mlg_password, self.login_path)

    def pull_national_data(self):
        time.sleep(3)
        if self.logged_in:
            self.driver.get(self.market_trends_path)
            time.sleep(3.5)
            frequency = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[1]/span")
            # frequency.send_keys("Quarterly\nselect")
            frequency.send_keys("Annual\nselect")

            report_level = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[2]/span")
            report_level.send_keys("National\nselect")

            start_quarter = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[3]/span[1]")
            start_quarter.send_keys("1st Quarter\nselect")

            start_year = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[3]/span[2]")
            start_year.send_keys("1995\nselect")

            end_quarter = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[4]/span[1]")
            end_quarter.send_keys("2nd Quarter\nselect")

            end_year = self.driver.find_element_by_xpath("//*[@id='body-container']/div/div[2]/div[1]/table/tbody/tr[1]/td[4]/span[2]")
            end_year.send_keys("2019\nselect")

            self.driver.find_element_by_id("btnMarketSearch").click()

            time.sleep(3)

            # now get table

            d_table = self.driver.find_element_by_xpath("//*[@id='period-wrap-table']")
            columns = d_table.find_elements(By.TAG_NAME, "td")

            index_col = columns[0].text.split("\n")
            m_list = [[i] for i in index_col]

            data_col = columns[1].text.split("\n")
            num_rows = len(index_col)

            s_rows = ["SUMMARY",
                      "PERFORMANCE TREND",
                      "Asking Rent",
                      "Effective Rent",
                      "Physical Occupancy Rate",
                      "Rental Revenue Impact",
                      "Concessions",
                      "Portfolio Attributes",
                      "SUPPLY AND DEMAND TREND",
                      "Job Growth",
                      "Residential Permitting",
                      "Job Growth Ratio",
                      "Single Family Home Affordability",
                      ]

            actual_data_length = len([i for i in index_col if i not in s_rows])

            quarter = 0
            r_count = 0
            d_count = 0
            while True:

                if r_count < num_rows and index_col[r_count] in s_rows:
                    m_list[r_count].append("")
                    r_count += 1
                else:
                    if r_count < num_rows and d_count < num_rows:
                        m_list[r_count].append(data_col[actual_data_length*quarter+d_count])
                        r_count += 1
                        d_count += 1
                    else:
                        if actual_data_length*quarter+d_count >= len(data_col):
                            break
                        r_count = 0
                        d_count = 0
                        quarter += 1
            pd.DataFrame(m_list)

    def navigate_to_property_report(self, id):
        path = os.path.join(self.property_report_path, str(id))
        self.driver.get(path)
        time.sleep(3)

    def get_property_details(self, id):
        """
        must call navigate_to_property_report prior to call
        :return:
        """

        # check if property id exists in db
        if session.query(exists().where(AxioProperty.property_id == str(id))).scalar():
            print
        else:
            property_details = {}
            property_details["property_id"] = id
            property_details["property_address"] = self.driver.find_element_by_css_selector(
                "#body-container > div > div.col-md-10 > div.page-header > table > tbody > tr > td:nth-child(1) > h2 > small").text
            property_details["property_name"] = self.driver.find_element_by_css_selector("#property-name").text
            property_details["property_owner"] = self.driver.find_element_by_css_selector(
                "#body-container > div > div.col-md-10 > div.page-header > table > tbody > tr > td:nth-child(2) > dl > dd:nth-child(4)").text
            property_details["property_management"] = self.driver.find_element_by_css_selector(
                "#body-container > div > div.col-md-10 > div.page-header > table > tbody > tr > td:nth-child(2) > dl > dd:nth-child(6)").text
            property_details["year_built"] = int(self.driver.find_element_by_css_selector(
                "#tab_unitmix > table:nth-child(3) > tbody > tr:nth-child(1) > td:nth-child(4)").text.split(":")[1])
            property_details["total_units"] = int(self.driver.find_element_by_css_selector(
                "#tab_unitmix > table:nth-child(3) > tbody > tr:nth-child(1) > td:nth-child(1)").text.split(":")[1])
            # property_details["total_square_feet"] = self.driver.find_element_by_css_selector("#property-name")

            axp = AxioProperty(**property_details)
            session.add(axp)
            session.commit()


    def get_property_data(self, id):
        """
        must call navigate_to_property_report prior to call
        :param id:
        :return:
        """

        occ = int(self.driver.find_element_by_css_selector("#tab_unitmix > table:nth-child(3) > tbody > tr:nth-child(1) > td:nth-child(2)").text.split(":")[1].replace("%", ""))/ 100
        tbl = self.driver.find_element_by_css_selector("#tab_unitmix > table:nth-child(5)")
        unit_report_list = []
        for row in tbl.find_elements_by_css_selector('tr'):
            tbl_width = len(row.text.split(" "))
            if tbl_width >= 14:
                d_tbl = row.find_elements_by_tag_name('td')
                if bool(d_tbl):
                    unit_report = {}
                    unit_report["property_id"] = id
                    unit_report["date_added"] = str(date.today())
                    for i, cell in enumerate(d_tbl):
                        if i == 0:
                            unit_report["type"] = cell.text.replace("/", "B/")
                        elif i == 1:
                            unit_report["area"] = cell.text.replace(",", "")
                            pass
                        elif i == 2:
                            unit_report["quantity"] = cell.text.replace(",", "")
                            pass
                        elif i == 5:
                            unit_report["avg_market_rent"] = cell.text.replace("$", "").replace(",", "")
                        elif i == 9:
                            unit_report["avg_effective_rent"] = cell.text.replace("$", "").replace(",", "")

                    if bool(unit_report):
                        unit_report_list.append(unit_report)

        for unit in unit_report_list:
            rc = RentComp(**unit)
            session.add(rc)
        session.commit()

class Costar:

    def __init__(self, asset_class, path):
        self.asset_class = asset_class
        self.path = path

    def import_data(self):
        df = pd.read_csv(self.path)
        if self.asset_class == "multifamily":
            self.multifamily_scrub(df)
        elif self.asset_class == "industrial":
            pass
        elif self.asset_class == "retail":
            pass
        elif self.asset_class == "office":
            pass

    def multifamily_scrub(self, df):
        slices = set(df["Slice"])

        master = {}
        for slice in slices:
            pass

    def office_scrub(self, df):
        pass

    def industrial_scrub(self, df):
        pass

    def retail_scrub(self, df):
        pass


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
    axio = Axio()
    axio.mlg_axio_login()
    # axio.pull_national_data()
    axio.navigate_to_property_report(51977)
    axio.get_property_details(51977)
    axio.get_property_data(51977)

    # costar_mf = Costar("multifamily", "data/costar/multifamily_all.csv")
    # costar_mf.import_data()
    # fetch_cbsa_population()
    # Case-Shiller Home Price Index


if __name__ == "__main__":
    main()

