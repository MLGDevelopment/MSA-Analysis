import pandas as pd
import os


class CostarMF:

    def __init__(self, path):
        self.curr_dir = os.path.dirname(os.path.realpath(__file__))
        self.path = path
        self.file_path = os.path.join(self.curr_dir, "data", "costar", self.path)
        self.df = pd.read_excel(self.file_path, converters={'CBSA': str})
        self.parse()

    def parse(self):
        self.df["Period"] = pd.to_datetime(self.df['Period'].str.replace(' ', '')) + pd.offsets.QuarterEnd(0)
        # GET SET OF CBSAS
        cbsas = list(set(self.df["CBSA Code"]))
        cbsa_df_dict = {}
        for cbsa in cbsas:
            temp_df = self.df[self.df["CBSA Code"] == cbsa]
            temp_df = temp_df.set_index("Period")
            grouped_df = temp_df.groupby(pd.PeriodIndex(temp_df.index, freq='Y'), axis=0).mean()
            cbsa_df_dict[cbsa] = grouped_df
        self.cbsa_grouped_dfs = cbsa_df_dict


if __name__ == "__main__":
    pass