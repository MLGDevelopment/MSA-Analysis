from datetime import datetime
import pandas as pd
import os


class Axio:

    def __init__(self):
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        self.load_monthly_markets_report()

    def load_monthly_markets_report(self):
        self.multifamily_path = os.path.join(self.curr_dir, "data", "datasets", "axio", "axio_monthly_market_data.xlsx")
        self.df_rents = pd.read_excel(self.multifamily_path, "Effective Rents", converters={'Market ID': str})
        self.df_occupancy = pd.read_excel(self.multifamily_path, "Occupancy", converters={'Market ID': str})

        cols_rents = self.df_rents.columns.values.tolist()[2:]
        cols_occupancy = self.df_occupancy.columns.values.tolist()[2:]

        date_cols_rents = []
        date_cols_occupancy = []

        for i, j in zip(cols_rents, cols_occupancy):
            date_cols_rents.append(datetime.strptime(i, "%b-%y"))
            date_cols_occupancy.append(datetime.strptime(j, "%b-%y"))

        self.df_rents.columns = self.df_rents.columns[:2].values.tolist() + date_cols_rents
        self.df_rents.set_index(self.df_rents.columns.values.tolist()[:2], drop=True, inplace=True)
        self.df_rents = self.df_rents.stack()
        self.df_rents = self.df_rents.reset_index(drop=False)
        self.df_rents.rename(columns={0: 'value', 'level_2': 'date'}, inplace=True)

        self.df_occupancy.columns = self.df_occupancy.columns[:2].values.tolist() + date_cols_occupancy
        self.df_occupancy.set_index(self.df_occupancy.columns.values.tolist()[:2], inplace=True, drop=True)
        self.df_occupancy = self.df_occupancy.stack()
        self.df_occupancy = self.df_occupancy.reset_index(drop=False)
        self.df_occupancy.rename(columns={0: 'value', 'level_2': 'date'}, inplace=True)

    def get_current_cbsa_rent(self, cbsa):

        if 'metro_id' in cbsa.keys():
            try:
                index = self.df_rents[self.df_rents['Market ID'] == cbsa['cbsa_id']]['date'].argmax()
                return self.df_rents.iloc[index, :]['value']
            except:
                # CBSA NOT FOUND, TRY METROS
                indexes = []
                try:
                    while cbsa['metro_id']:
                        cbsa_id = cbsa['metro_id'].pop()
                        index = self.df_rents[self.df_rents['Market ID'] == cbsa_id]['date'].argmax()
                        indexes.append(index)

                    return self.df_rents.iloc[indexes, :]['value'].mean()
                except:
                    return -1

        else:
            try:
                index = self.df_rents[self.df_rents['Market ID'] == cbsa['cbsa_id']]['date'].argmax()
                return self.df_rents.iloc[index, :]['value']
            except:
                return -1


    def get_12_month_rent_cbsa_growth(self, cbsa):
        try:
            index = self.df_rents[self.df_rents['Market ID'] == cbsa]['date'].argmax()
            ttm_index = index - 13
            return self.df_rents.iloc[index, :]['value'] / self.df_rents.iloc[ttm_index, :]['value'] - 1
        except:
            return -1

    def get_current_cbsa_occupancy(self, cbsa):
        try:
            index = self.df_occupancy[self.df_occupancy['Market ID'] == cbsa]['date'].argmax()
            return self.df_occupancy.iloc[index, :]['value']
        except:
            return -1


if __name__ == "__main__":
    axio = Axio()
    axio.get_current_cbsa_rent('35614')
