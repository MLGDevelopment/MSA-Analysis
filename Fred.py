import pandas as pd
from fredapi import Fred
import time
import ast
from urllib.request import urlopen
import requests
import io
import os
import json
import urllib.request
import urllib

import datetime

import altair as alt


FRED_API = "7b01ffec8a8b5fa92a16c5af3f72ff14"





class FredLayer():

    def __init__(self):
        self.fred_engine = Fred(api_key=FRED_API)

    def get_usd_series(self):
        sid = "DTWEXBGS"
        min_date = pd.to_datetime("2006-01-02")
        self.usd_series = self.fred_engine.get_series(sid)
        self.usd_series = self.usd_series[self.usd_series.index > min_date]

        self.usd_series.columns = ['date,' 'USD Price Index']

    def get_purchase_only_house_price_index_us(self):
        sid = "HPIPONM226S"
        min_date = pd.to_datetime("2006-01-02")
        self.purchase_only_house_price_index_us_series = self.fred_engine.get_series(sid)
        self.purchase_only_house_price_index_us_series = \
            self.purchase_only_house_price_index_us_series[self.purchase_only_house_price_index_us_series.index >
                                                           min_date]

        self.columns = ['date', 'Purchases Only House Price Index']

    def get_steel_product_manufacturing_from_purchased_steel(self):
        """
        https://fred.stlouisfed.org/series/PCU33123312
        :return:
        """
        sid = "PCU33123312"
        self.steel_product_manufacturing_from_purchased_steel = self.fred_engine.get_series(sid)

    def get_producer_price_index_hardwood_lumber(self):
        """
        https://fred.stlouisfed.org/series/PCU3211133211131
        :return:
        """
        sid = "PCU3211133211131"
        self.producer_price_index__hardwood_lumber = self.fred_engine.get_series(sid)

    def get_single_family_median_sale_price(self):
        """

        :return:
        """
        sid = "MSPUS"
        self.single_family_median_sale_price = self.fred_engine.get_series(sid)

    def get_owners_equity_in_real_estate(self):
        """

        :return:
        """
        sid = "OEHRENWBSHNO"
        self.owners_equity_in_real_estate = self.fred_engine.get_series(sid)

    def get_monthlY_supply_of_houses(self):
        """
        https://fred.stlouisfed.org/series/MSACSR
        :return:
        """
        sid = "MSACSR"
        self.owners_equity_in_real_estate = self.fred_engine.get_series(sid)

    def get_periods_of_us_recessions(self):
        """

        :return:
        """
        sid = "JHDUSRGDPBR"
        self.periods_of_us_recessions = self.fred_engine.get_series(sid)


    def get_moodys_AAA_bond_yields(self):
        """

        :return:
        """
        sid = "DAAA"
        self.moodys_AAA_bond_yields = self.fred_engine.get_series(sid)



    def plot_time_series(self, series={}):
        """
        A function to plot time serieses
        :param series:
        :return:
        """

        print

    def main(self):
        self.get_usd_series()
        self.get_purchase_only_house_price_index_us()
        df = pd.DataFrame([self.usd_series.index.values, self.usd_series.values]).T
        df.columns = ['x', 'y']

        self.plot_time_series()
        print

if __name__ == "__main__":
    fred_layer = FredLayer()
    fred_layer.main()
    print
