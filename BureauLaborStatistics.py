import os
import urllib.request
import urllib
import pandas as pd
import requests
import json

"""
https://beta.bls.gov/dataQuery/find?q=unemployment

BLS Survey's:
https://www.bls.gov/data/
- CPS: https://www.bls.gov/cps/
- QCEW: https://www.bls.gov/cew/
- 


"""

all_counties_national_columns = ['area_fips', 'own_code', 'industry_code', 'agglvl_code', 'size_code', 'year', 'qtr',
                                 'disclosure_code', 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages',
                                 'taxable_annual_wages', 'annual_contributions', 'annual_avg_wkly_wage',
                                 'avg_annual_pay']



class BLS:

    """
    https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm
    """



    def __init__(self):
        # TODO: MOVE TO MAPPINGS
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        self.stored_data = os.path.join(self.curr_dir, "data", "stored_data")
        self.report_features_path = os.path.join(self.curr_dir, "data", "mappings", "BLSReportList.xlsx")
        self.bls_report_list = pd.read_excel(self.report_features_path)

        self.bls_dir = os.path.join(self.curr_dir, 'data', 'bls')
        self.mappings = os.path.join(self.curr_dir, "data", "mappings")
        self.cbsa_ssafips = os.path.join(self.mappings, "CBSA_SSAFIPS.xlsx")
        self.cbsa_ssa = pd.read_excel(self.cbsa_ssafips, dtype={"CBSA": str,
                                                                "SSA": str,
                                                                "FIPS": str})



    def qcewCreateDataRows(self, csv):
        dataRows = []
        try:
            dataLines = csv.decode().split('\r\n')
        except:
            dataLines = csv.split('\r\n');
        for row in dataLines:
            dataRows.append(row.split(','))
        return dataRows

    def qcewGetAreaData(self, year, qtr, area):
        urlPath = "http://data.bls.gov/cew/data/api/[YEAR]/[QTR]/area/[AREA].csv"
        urlPath = urlPath.replace("[YEAR]", year)
        urlPath = urlPath.replace("[QTR]", qtr.lower())
        urlPath = urlPath.replace("[AREA]", area.upper())
        httpStream = urllib.request.urlopen(urlPath)
        csv = httpStream.read()
        httpStream.close()
        return self.qcewCreateDataRows(csv)

    def qcewGetIndustryData(self, year, qtr, industry):
        urlPath = "http://data.bls.gov/cew/data/api/[YEAR]/[QTR]/industry/[IND].csv"
        urlPath = urlPath.replace("[YEAR]", year)
        urlPath = urlPath.replace("[QTR]", qtr.lower())
        urlPath = urlPath.replace("[IND]", industry)
        httpStream = urllib.request.urlopen(urlPath)
        csv = httpStream.read()
        httpStream.close()
        return self.qcewCreateDataRows(csv)

    def qcewGetSizeData(self, year, size):
        urlPath = "http://data.bls.gov/cew/data/api/[YEAR]/1/size/[SIZE].csv"
        urlPath = urlPath.replace("[YEAR]", year)
        urlPath = urlPath.replace("[SIZE]", size)
        httpStream = urllib.request.urlopen(urlPath)
        csv = httpStream.read()
        httpStream.close()
        return self.qcewCreateDataRows(csv)

    def bls_industry_data(self, years=[], filter=[], export=False):
        """
        Codes pulled from:
        - https://www.bls.gov/cew/classifications/industry/industry-supersectors.htm
        - https://www.bls.gov/cew/classifications/aggregation/agg-level-titles.htm
        - https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm
        ownership codes: https://www.bls.gov/cew/classifications/ownerships/sic-ownership-titles.htm

        10	    Total, all industries
        101	    Goods-Producing
        1011	Natural Resources and Mining
        1012	Construction
        1013	Manufacturing
        102	    Service-Providing
        1021	Trade, Transportation, and Utilities
        1022	Information
        1023	Financial Activities
        1024	Professional and Business Services
        1025	Education and Health Services
        1026	Leisure and Hospitality
        1027	Other Services
        1028	Public Administration
        1029	Unclassified

        :return:
        """

        self.cew_codes_mapping = {"10": "Total, all industries",
                                  "101": "Goods-Producing",
                                  "1011": "Natural Resources and Mining",
                                  "1012": "Construction",
                                  "1013": "Manufacturing",
                                  "102": "Service-Providing",
                                  "1021": "Trade, Transportation, and Utilities",
                                  "1022": "Information",
                                  "1023": "Financial Activities",
                                  "1024": "Professional and Business Services",
                                  "1025": "Education and Health Services",
                                  "1026": "Leisure and Hospitality",
                                  "1027": "Other Services",
                                  "1028": "Public Administration",
                                  "1029": "Unclassified"}
        self.cew_codes = [10, 101, 1011, 1012, 1013, 102, 1021, 1022, 1023, 1024, 1025,
                          1026, 1027, 1028, 1029]

        if not years:
            years = list(range(2019, 2020))
            years.reverse()

        headers = ['area_fips', 'own_code', 'industry_code', 'agglvl_code', 'size_code', 'year', 'qtr',
                   'disclosure_code', 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages',
                   'taxable_annual_wages', 'annual_contributions', 'annual_avg_wkly_wage', 'avg_annual_pay',
                   'lq_disclosure_code', 'lq_annual_avg_estabs', 'lq_annual_avg_emplvl', 'lq_total_annual_wages',
                   'lq_taxable_annual_wages', 'lq_annual_contributions', 'lq_annual_avg_wkly_wage', 'lq_avg_annual_pay',
                   'oty_disclosure_code', 'oty_annual_avg_estabs_chg', 'oty_annual_avg_estabs_pct_chg',
                   'oty_annual_avg_emplvl_chg', 'oty_annual_avg_emplvl_pct_chg', 'oty_total_annual_wages_chg',
                   'oty_total_annual_wages_pct_chg', 'oty_taxable_annual_wages_chg', 'oty_taxable_annual_wages_pct_chg',
                   'oty_annual_contributions_chg', 'oty_annual_contributions_pct_chg', 'oty_annual_avg_wkly_wage_chg',
                   'oty_annual_avg_wkly_wage_pct_chg', 'oty_avg_annual_pay_chg', 'oty_avg_annual_pay_pct_chg']

        _master = pd.DataFrame(columns=headers)
        for code in self.cew_codes:
            for year in years:
                r = self.qcewGetIndustryData(str(year), str('a'), str(code))[1:]
                r = [[j.replace('\"', "") for j in i] for i in r]
                _df = pd.DataFrame(r, columns=headers)
                _master = _master.append(_df)

        _master.iloc[:, 8:] = _master.iloc[:, 8:].apply(pd.to_numeric, errors='coerce')

        if filter:
            self.df = _master[filter]
        else:
            self.df = _master

        self.df["industry_code_name"] = self.df["industry_code"].replace(self.cew_codes_mapping)

        if export:
            self.df.to_excel(os.path.join(self.bls_dir, "bls_county_data.xlsx"))

    def fetch_bls_datasets(self, series, year_start, year_end, export=False):

        year_span = range(year_start, year_end+1)
        headers = {'Content-type': 'application/json'}
        data = json.dumps(series)
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(p.text)
        master = {}
        for series in json_data['Results']['series']:
            x = [["series id", "year", "period", "value"]]
            seriesId = series['seriesID']
            for item in series['data']:
                year = item['year']
                period = item['period']
                value = float(item['value'])
                x.append([seriesId, year, period, value])

            master[series['seriesID']] = x

        return master

def main():
    bls = BLS()
    # series = {"seriesid": ['CUUR0000SA0', 'SUUR0000SA0'], "startyear": "2000", "endyear": "2020"}
    report_list = bls.bls_report_list['report id'].values.tolist()
    series = {"seriesid": report_list,
              "startyear": "2000",
              "endyear": "2020",
              "registrationkey": "e2673e2e9de5482d9f9ad79fc059ba2a"}
    df = bls.fetch_bls_datasets(series, year_start=2000, year_end=2020, export=True)



    print
    # bls.bls_industry_data(export=1)


if __name__ == "__main__":
    main()