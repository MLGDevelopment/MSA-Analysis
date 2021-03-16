import pandas as pd
import os


class Costar:

    def __init__(self):
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        self.multifamily_path = os.path.join(self.curr_dir, "data", "datasets", "costar", "multifamily", "costar_multifamily_export.xlsx")
        self.office_path = os.path.join(self.curr_dir, "data", "datasets", "office", "costar_office_export.xlsx")
        self.retail_path = os.path.join(self.curr_dir, "data", "datasets", "retail", "costar_retail_export.xlsx")
        self.industrial_path = os.path.join(self.curr_dir, "data", "datasets", "industrial", "co_star_retail_export.xlsx")

        # self.path = path
        # self.file_path = os.path.join(self.curr_dir, "data", "costar", self.path)


class CostarMultifamily(Costar):

    def __init__(self):
        Costar.__init__(self)
        self.df = pd.read_excel(self.multifamily_path, converters={'CBSA Code': str})
        self.load_multifamily_market_report()

    def load_multifamily_market_report(self):
        # convert quarters to dates
        self.df["As of date"] = pd.to_datetime(self.df['Period'].str.replace(' ', '')) + pd.offsets.QuarterEnd(0)

    def get_most_recent_cbsa_rent(self, cbsa):
        try:
            return self.df[self.df['CBSA Code'] == cbsa]["Market Effective Rent/Unit"].iloc[-1]
        except:
            return -1

    def get_most_recent_occupancy(self, cbsa):
        try:
            return self.df[self.df['CBSA Code'] == cbsa]["Occupancy Rate"].iloc[-1]
        except:
            return -1

    def get_12_month_effective_rent_growth(self, cbsa):
        try:
            return self.df[self.df['CBSA Code'] == cbsa]["Market Effective Rent Growth 12 Mo"].iloc[-1]
        except:
            return -1


class CostarIndustrial(Costar):

    def __init__(self):
        pass


class CostarOffice(Costar):

    def __init__(self):
        pass


class CostarRetail(Costar):

    def __init__(self):
        pass


if __name__ == "__main__":
    costar_mf = CostarMultifamily()