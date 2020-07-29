from DataScraper import CensusBureau
from DataScraper import Mappings
import pandas as pd
import os
from datetime import datetime
from Costar import CostarMF

mappings = Mappings()


def write_multiple_dfs(writer, df_list, sheets, spaces):
    col = 0
    for dataframe in df_list:
        # dataframe.iloc[:,1] = pd.Series(["{0:.2f}%".format(val * 100) for val in dataframe.iloc[:,1]], index=dataframe.index)
        dataframe.to_excel(writer, sheet_name=sheets, startrow=0, startcol=col)
        col = col + len(dataframe.columns) + spaces + 1
    writer.save()


def replace_ranks(df):
    # REPLACE WITH [-5, 5]
    df = df.replace(0, -5)
    df = df.replace(1, -4)
    df = df.replace(2, -3)
    df = df.replace(3, -2)
    df = df.replace(4, -1)
    df = df.replace(5, 1)
    df = df.replace(6, 2)
    df = df.replace(7, 3)
    df = df.replace(8, 4)
    df = df.replace(9, 5)
    return df

def analyze_population(cb, costar_mf, export, plot):
    """
    Algorithm for analyzing population data in the US

    DATA DOCUMENTATION:
    https://www.census.gov/programs-surveys/popest/about/glossary.html
    https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cbsa-est2019-alldata.pdf
    :param cb:
    :return:
    """

    # HARDCODED POPULATION THRESHOLD - SET TO 100,000
    POPULATION_THRESHOLD = 100000
    THRESHOLD_TOP = .10
    THRESHOLD_BOTTOM = .10

    features = cb.pop_features
    df = cb.fetch_population_csv()
    df = df[df["POPESTIMATE2019"] >= POPULATION_THRESHOLD]
    _2019_MEDIAN = df["POPESTIMATE2019"].median()
    # pd.qcut(df["POPESTIMATE2019"], 10, labels=False)
    CBSA_NAMES = df[["CBSA", "NAME"]][
        (df["LSAD"] == "Metropolitan Statistical Area") | (df["LSAD"] == "Micropolitan Statistical Area")].set_index(
        "CBSA")
    mappings.cbsa_fips_df = mappings.cbsa_fips_df[["cbsacode", "fipsstatecode"]].drop_duplicates()
    # AGGREGATE ON CBSA CODE
    df = df[(df["LSAD"] == "Metropolitan Statistical Area") | (df["LSAD"] == "Micropolitan Statistical Area")]
    df = df.groupby("CBSA").sum()

    """
    START RELATIVE ANALYSIS
    """

    df_relative_analysis = pd.DataFrame(index=df.index)

    # COMPUTE ANNUAL PERCENT CHANGE
    df_relative_analysis["POPULATION_%CHG_2011"] = df["POPESTIMATE2011"] / df["POPESTIMATE2010"] - 1
    df_relative_analysis["POPULATION_%CHG_2012"] = df["POPESTIMATE2012"] / df["POPESTIMATE2011"] - 1
    df_relative_analysis["POPULATION_%CHG_2013"] = df["POPESTIMATE2013"] / df["POPESTIMATE2012"] - 1
    df_relative_analysis["POPULATION_%CHG_2014"] = df["POPESTIMATE2014"] / df["POPESTIMATE2013"] - 1
    df_relative_analysis["POPULATION_%CHG_2015"] = df["POPESTIMATE2015"] / df["POPESTIMATE2014"] - 1
    df_relative_analysis["POPULATION_%CHG_2016"] = df["POPESTIMATE2016"] / df["POPESTIMATE2015"] - 1
    df_relative_analysis["POPULATION_%CHG_2017"] = df["POPESTIMATE2017"] / df["POPESTIMATE2016"] - 1
    df_relative_analysis["POPULATION_%CHG_2018"] = df["POPESTIMATE2018"] / df["POPESTIMATE2017"] - 1
    df_relative_analysis["POPULATION_%CHG_2019"] = df["POPESTIMATE2019"] / df["POPESTIMATE2018"] - 1

    # LOOK AT ROC OF ANNUAL GROWTH
    df_relative_analysis["NPOPCHG2011_%CHG"] = df["NPOPCHG2011"] / df["NPOPCHG2010"] - 1
    df_relative_analysis["NPOPCHG2012_%CHG"] = df["NPOPCHG2012"] / df["NPOPCHG2011"] - 1
    df_relative_analysis["NPOPCHG2013_%CHG"] = df["NPOPCHG2013"] / df["NPOPCHG2012"] - 1
    df_relative_analysis["NPOPCHG2014_%CHG"] = df["NPOPCHG2014"] / df["NPOPCHG2013"] - 1
    df_relative_analysis["NPOPCHG2015_%CHG"] = df["NPOPCHG2015"] / df["NPOPCHG2014"] - 1
    df_relative_analysis["NPOPCHG2016_%CHG"] = df["NPOPCHG2016"] / df["NPOPCHG2015"] - 1
    df_relative_analysis["NPOPCHG2017_%CHG"] = df["NPOPCHG2017"] / df["NPOPCHG2016"] - 1
    df_relative_analysis["NPOPCHG2018_%CHG"] = df["NPOPCHG2018"] / df["NPOPCHG2017"] - 1
    df_relative_analysis["NPOPCHG2019_%CHG"] = df["NPOPCHG2019"] / df["NPOPCHG2018"] - 1

    # COMPUTE POPULATION AVERAGES: 2, 3, 5, ALL
    _2_YEAR_POP_SLICE = features[15:17]
    _3_YEAR_POP_SLICE = features[14:17]
    _5_YEAR_POP_SLICE = features[12:17]
    _ALL_YEAR_POP_SLICE = features[7:17]
    _2_YEAR_POP_AVG = df[df.columns.intersection(_2_YEAR_POP_SLICE)].mean(axis=1)
    _3_YEAR_POP_AVG = df[df.columns.intersection(_3_YEAR_POP_SLICE)].mean(axis=1)
    _5_YEAR_POP_AVG = df[df.columns.intersection(_5_YEAR_POP_SLICE)].mean(axis=1)
    _ALL_YEAR_POP_AVG = df[df.columns.intersection(_ALL_YEAR_POP_SLICE)].mean(axis=1)

    # 2 AND 3 YEAR AND 5 YEAR POPULATION CHANGE AVERAGES
    _2_YEAR_CHANGE_SLICE = features[25:27]
    _3_YEAR_CHANGE_SLICE = features[24:27]
    _5_YEAR_CHANGE_SLICE = features[22:27]
    _ALL_YEAR_CHANGE_SLICE = features[17:27]
    df_relative_analysis["_2_YEAR_POPULATION_RATE_CHANGE"] = df[df.columns.intersection(_2_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["_3_YEAR_POPULATION_RATE_CHANGE"] = df[df.columns.intersection(_3_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["_5_YEAR_POPULATION_RATE_CHANGE"] = df[df.columns.intersection(_5_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["_ALL_YEAR_POPULATION_RATE_CHANGE"] = df[df.columns.intersection(_ALL_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # DOMESTIC MIGRATION
    _2_YEAR_DOMESTIC_MIGRATION = features[75:77]
    _3_YEAR_DOMESTIC_MIGRATION = features[74:77]
    _5_YEAR_DOMESTIC_MIGRATION = features[72:77]
    _ALL_YEAR_DOMESTIC_MIGRATION = features[67:77]
    df_relative_analysis["2_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_2_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # INTERNATIONAL MIGRATION
    _2_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[65:67]
    _3_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[64:67]
    _5_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[62:67]
    _ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[57:67]
    df_relative_analysis["2_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(
        _2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(
        _3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(
        _5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(
        _ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # MOST DEATHS 2, 3, 5, ALL YEARS
    _2_YEAR_DEATHS_SLICE = features[45:47]
    _3_YEAR_DEATHS_SLICE = features[44:47]
    _5_YEAR_DEATHS_SLICE = features[42:47]
    _ALL_YEAR_DEATHS_SLICE = features[37:47]
    df_relative_analysis["2_YEAR_DEATHS"] = df[df.columns.intersection(_2_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DEATHS"] = df[df.columns.intersection(_3_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DEATHS"] = df[df.columns.intersection(_5_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DEATHS"] = df[df.columns.intersection(_ALL_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # MOST BIRTHS 2, 3, 5, ALL YEARS
    _2_YEAR_BIRTHS_SLICE = features[35:37]
    _3_YEAR_BIRTHS_SLICE = features[34:37]
    _5_YEAR_BIRTHS_SLICE = features[32:37]
    _ALL_YEAR_BIRTHS_SLICE = features[27:37]
    df_relative_analysis["2_YEAR_BIRTHS"] = df[df.columns.intersection(_2_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_BIRTHS"] = df[df.columns.intersection(_3_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_BIRTHS"] = df[df.columns.intersection(_5_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_BIRTHS"] = df[df.columns.intersection(_ALL_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # PERCENT OF NATURAL INCREASE FROM BIRTHS OR DEATHS
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2011"] = df["BIRTHS2011"] / df["NATURALINC2010"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2012"] = df["BIRTHS2012"] / df["NATURALINC2011"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2013"] = df["BIRTHS2013"] / df["NATURALINC2012"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2014"] = df["BIRTHS2014"] / df["NATURALINC2013"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2015"] = df["BIRTHS2015"] / df["NATURALINC2014"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2016"] = df["BIRTHS2016"] / df["NATURALINC2015"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2017"] = df["BIRTHS2017"] / df["NATURALINC2016"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2018"] = df["BIRTHS2018"] / df["NATURALINC2017"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2019"] = df["BIRTHS2019"] / df["NATURALINC2018"] - 1

    df_relative_analysis["PER_DEATHS_NAT_MIG_2011"] = df["DEATHS2011"] / df["NATURALINC2010"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2012"] = df["DEATHS2012"] / df["NATURALINC2011"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2013"] = df["DEATHS2013"] / df["NATURALINC2012"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2014"] = df["DEATHS2014"] / df["NATURALINC2013"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2015"] = df["DEATHS2015"] / df["NATURALINC2014"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2016"] = df["DEATHS2016"] / df["NATURALINC2015"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2017"] = df["DEATHS2017"] / df["NATURALINC2016"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2018"] = df["DEATHS2018"] / df["NATURALINC2017"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2019"] = df["DEATHS2019"] / df["NATURALINC2018"] - 1

    # PERCENT OF NET MIGRATION FROM DOMESTIC AND INTERNATIONAL
    df_relative_analysis["PER_DOMESTIC_MIG_2011"] = df["DOMESTICMIG2011"] / df["NETMIG2010"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2012"] = df["DOMESTICMIG2012"] / df["NETMIG2011"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2013"] = df["DOMESTICMIG2013"] / df["NETMIG2012"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2014"] = df["DOMESTICMIG2014"] / df["NETMIG2013"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2015"] = df["DOMESTICMIG2015"] / df["NETMIG2014"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2016"] = df["DOMESTICMIG2016"] / df["NETMIG2015"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2017"] = df["DOMESTICMIG2017"] / df["NETMIG2016"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2018"] = df["DOMESTICMIG2018"] / df["NETMIG2017"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2019"] = df["DOMESTICMIG2019"] / df["NETMIG2018"] - 1

    df_relative_analysis["PER_INTERNATIONAL_MIG_2011"] = df["INTERNATIONALMIG2011"] / df["NETMIG2010"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2012"] = df["INTERNATIONALMIG2012"] / df["NETMIG2011"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2013"] = df["INTERNATIONALMIG2013"] / df["NETMIG2012"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2014"] = df["INTERNATIONALMIG2014"] / df["NETMIG2013"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2015"] = df["INTERNATIONALMIG2015"] / df["NETMIG2014"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2016"] = df["INTERNATIONALMIG2016"] / df["NETMIG2015"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2017"] = df["INTERNATIONALMIG2017"] / df["NETMIG2016"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2018"] = df["INTERNATIONALMIG2018"] / df["NETMIG2017"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2019"] = df["INTERNATIONALMIG2019"] / df["NETMIG2018"] - 1

    """
    START ABSOLUTE ANALYSIS
    """

    df_abs_analysis = pd.DataFrame(index=df.index)

    # AGGREGATE ABSOLUTES: 1, 3, 5, ALL YEARS
    df_abs_analysis["1_YEAR_ABS_CHANGE"] = df["NPOPCHG2019"]
    df_abs_analysis["2_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_2_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_3_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_5_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_ALL_YEAR_CHANGE_SLICE)].sum(axis=1)

    # BIRTHS
    df_abs_analysis["1_YEAR_ABS_BIRTHS"] = df["BIRTHS2019"]
    df_abs_analysis["3_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_2_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_3_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_5_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_ALL_YEAR_BIRTHS_SLICE)].sum(axis=1)

    # DEATHS
    df_abs_analysis["1_YEAR_ABS_DEATHS"] = df["DEATHS2019"]
    df_abs_analysis["2_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_2_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_3_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_5_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_ALL_YEAR_DEATHS_SLICE)].sum(axis=1)

    # DOMESTIC
    df_abs_analysis["1_YEAR_ABS_DOMESTIC"] = df["DOMESTICMIG2019"]
    df_abs_analysis["2_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_2_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_ALL_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)

    # INTERNATIONAL
    df_abs_analysis["1_YEAR_ABS_INTERNATIONAL"] = df["INTERNATIONALMIG2019"]
    df_abs_analysis["2_YEAR_ABS_INTERNATIONAL"] = df[
        df.columns.intersection(_2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_INTERNATIONAL"] = df[
        df.columns.intersection(_3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"] = df[
        df.columns.intersection(_5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_INTERNATIONAL"] = df[
        df.columns.intersection(_ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)

    df_abs_analysis["FIPS"] = df_abs_analysis.index.map(dict(mappings.cbsa_fips_df.values))
    df_abs_analysis["STATE"] = df_abs_analysis["FIPS"].map(mappings.states_df["state"])

    df_relative_analysis["FIPS"] = df_abs_analysis.index.map(dict(mappings.cbsa_fips_df.values))
    df_relative_analysis["STATE"] = df_abs_analysis["FIPS"].map(mappings.states_df["state"])

    df_abs_states = df_abs_analysis.groupby("STATE").sum()

    # ASSIGN RANKINGS
    # DO RELATIVE FIRST
    df_all_rankings = pd.DataFrame(index=df.index)
    for col in df_relative_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_relative_analysis[col], 10, labels=False)

    for col in df_abs_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_abs_analysis[col], 10, labels=False)

    df_all_rankings["CBSA NAME"] = CBSA_NAMES

    df_consolidated_rankings = pd.DataFrame(index=df.index)
    df_consolidated_rankings["CBSA NAME"] = CBSA_NAMES

    df_consolidated_rankings["2019 Pop Estimate"] = df["POPESTIMATE2019"]
    df_consolidated_rankings["3 Year Growth Rank"] = df_all_rankings["_3_YEAR_POPULATION_RATE_CHANGE_RANK"]
    df_consolidated_rankings["5 Year Growth Rank"] = df_all_rankings["_5_YEAR_POPULATION_RATE_CHANGE_RANK"]
    df_consolidated_rankings["10 Year Growth Rank"] = df_all_rankings["_ALL_YEAR_POPULATION_RATE_CHANGE_RANK"]
    sum_list = ["3 Year Growth Rank", "5 Year Growth Rank", "10 Year Growth Rank"]
    df_consolidated_rankings["Total Growth Rank"] = df_consolidated_rankings[sum_list].sum(axis=1)
    df_consolidated_rankings["3 Year Dom. Growth Rank"] = df_all_rankings["3_YEAR_DOMESTIC_MIGRATION_RANK"]
    df_consolidated_rankings["5 Year Dom. Growth Rank"] = df_all_rankings["5_YEAR_DOMESTIC_MIGRATION_RANK"]
    df_consolidated_rankings["10 Year Dom. Growth Rank"] = df_all_rankings["ALL_YEAR_DOMESTIC_MIGRATION_RANK"]
    sum_list = ["3 Year Dom. Growth Rank", "5 Year Dom. Growth Rank", "10 Year Dom. Growth Rank"]
    df_consolidated_rankings["Total Dom. Rank"] = df_consolidated_rankings[sum_list].sum(axis=1)
    df_consolidated_rankings["3 Year Int. Growth Rank"] = df_all_rankings["3_YEAR_INTERNATIONAL_MIGRATION_RANK"]
    df_consolidated_rankings["5 Year Int. Growth Rank"] = df_all_rankings["5_YEAR_INTERNATIONAL_MIGRATION_RANK"]
    df_consolidated_rankings["10 Year Int. Growth Rank"] = df_all_rankings["ALL_YEAR_INTERNATIONAL_MIGRATION_RANK"]
    sum_list = ["3 Year Int. Growth Rank", "5 Year Int. Growth Rank", "10 Year Int. Growth Rank"]
    df_consolidated_rankings["Total Int. Rank"] = df_consolidated_rankings[sum_list].sum(axis=1)

    t_cbsas = list(costar_mf.cbsa_grouped_dfs.keys())
    median_income_map = {}
    for cbsa in t_cbsas:
        median_income_map[cbsa] = {"2019 Median HHI": costar_mf.cbsa_grouped_dfs[cbsa]["Median Household Income"].filter(like="2019").values[0],
                                   "2016 Median HHI": costar_mf.cbsa_grouped_dfs[cbsa]["Median Household Income"].filter(like="2016").values[0],
                                   "2014 Median HHI": costar_mf.cbsa_grouped_dfs[cbsa]["Median Household Income"].filter(like="2014").values[0],
                                   "2010 Median HHI": costar_mf.cbsa_grouped_dfs[cbsa]["Median Household Income"].filter(like="2009").values[0]}

    median_inc_map_df = pd.DataFrame.from_dict(median_income_map, orient='index')
    df_consolidated_rankings = df_consolidated_rankings.join(median_inc_map_df)


    # RANKINGS & REPORTS
    df_relative_analysis["CBSA NAME"] = CBSA_NAMES
    df_abs_states["CBSA NAME"] = CBSA_NAMES
    df_abs_analysis["CBSA NAME"] = CBSA_NAMES

    # GET FASTEST GROWING/DECLINING YOY
    FASTEST_GROWING_YOY = df_relative_analysis.sort_values(by=["POPULATION_%CHG_2019"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "POPULATION_%CHG_2019"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_YOY = df_relative_analysis.sort_values(by=["POPULATION_%CHG_2019"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_BOTTOM))[["CBSA NAME", "POPULATION_%CHG_2019"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_2_YEAR = \
    df_relative_analysis.sort_values(by=["_2_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "_2_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_YEAR = \
    df_relative_analysis.sort_values(by=["_2_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_BOTTOM))[["CBSA NAME", "_2_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_YEAR = \
    df_relative_analysis.sort_values(by=["_3_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "_3_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_YEAR = \
    df_relative_analysis.sort_values(by=["_3_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_BOTTOM))[["CBSA NAME", "_3_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_YEAR = \
    df_relative_analysis.sort_values(by=["_5_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "_5_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_YEAR = \
    df_relative_analysis.sort_values(by=["_5_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_BOTTOM))[["CBSA NAME", "_5_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_YEAR = \
    df_relative_analysis.sort_values(by=["_ALL_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "_ALL_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_YEAR = \
    df_relative_analysis.sort_values(by=["_ALL_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_BOTTOM))[["CBSA NAME", "_ALL_YEAR_POPULATION_RATE_CHANGE"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])

    # BUILD DOMESTIC TABLES
    FASTEST_GROWING_2_DOMESTIC = \
    df_relative_analysis.sort_values(by=["2_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_DOMESTIC = \
    df_relative_analysis.sort_values(by=["2_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_DOMESTIC = \
    df_relative_analysis.sort_values(by=["3_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_DOMESTIC = \
    df_relative_analysis.sort_values(by=["3_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_DOMESTIC = \
    df_relative_analysis.sort_values(by=["5_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_DOMESTIC = \
    df_relative_analysis.sort_values(by=["5_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_DOMESTIC = \
    df_relative_analysis.sort_values(by=["ALL_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_DOMESTIC = \
    df_relative_analysis.sort_values(by=["ALL_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_DOMESTIC_MIGRATION"]].join(
        df[["POPESTIMATE2019", "NPOPCHG2019"]])

    FASTEST_GROWING_2_INT = \
        df_relative_analysis.sort_values(by=["2_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_INT = \
        df_relative_analysis.sort_values(by=["2_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_INT = \
        df_relative_analysis.sort_values(by=["3_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_INT = \
        df_relative_analysis.sort_values(by=["3_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_INT = \
        df_relative_analysis.sort_values(by=["5_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_INT = \
        df_relative_analysis.sort_values(by=["5_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_INT = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_INT = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])

    # BUILD BIRTHS AND DEATHS
    FASTEST_GROWING_2_BIRTHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_BIRTHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_BIRTHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_BIRTHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_BIRTHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_BIRTHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_BIRTHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_BIRTHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_BIRTHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])

    FASTEST_GROWING_2_DEATHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_DEATHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "2_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_DEATHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_DEATHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "3_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_DEATHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_DEATHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "5_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_DEATHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_DEATHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * THRESHOLD_TOP))[["CBSA NAME", "ALL_YEAR_DEATHS"]].join(
            df[["POPESTIMATE2019", "NPOPCHG2019"]])

    # TODO: MAP OD DEATHS TO CBSA TO GAUGE CONTRIBUTION TO DEATH RATES

    all_df_reports = [FASTEST_GROWING_YOY,
                      FASTEST_DECLINING_YOY,
                      FASTEST_GROWING_2_YEAR,
                      FASTEST_DECLINING_2_YEAR,
                      FASTEST_GROWING_3_YEAR,
                      FASTEST_DECLINING_3_YEAR,
                      FASTEST_GROWING_5_YEAR,
                      FASTEST_DECLINING_5_YEAR,
                      FASTEST_GROWING_ALL_YEAR,
                      FASTEST_DECLINING_ALL_YEAR,
                      FASTEST_GROWING_2_DOMESTIC,
                      FASTEST_DECLINING_2_DOMESTIC,
                      FASTEST_GROWING_3_DOMESTIC,
                      FASTEST_DECLINING_3_DOMESTIC,
                      FASTEST_GROWING_5_DOMESTIC,
                      FASTEST_DECLINING_5_DOMESTIC,
                      FASTEST_GROWING_ALL_DOMESTIC,
                      FASTEST_DECLINING_ALL_DOMESTIC,
                      FASTEST_GROWING_2_INT,
                      FASTEST_DECLINING_2_INT,
                      FASTEST_GROWING_3_INT,
                      FASTEST_DECLINING_3_INT,
                      FASTEST_GROWING_5_INT,
                      FASTEST_DECLINING_5_INT,
                      FASTEST_GROWING_ALL_INT,
                      FASTEST_DECLINING_ALL_INT,
                      FASTEST_GROWING_2_BIRTHS,
                      FASTEST_DECLINING_2_BIRTHS,
                      FASTEST_GROWING_3_BIRTHS,
                      FASTEST_DECLINING_3_BIRTHS,
                      FASTEST_GROWING_5_BIRTHS,
                      FASTEST_DECLINING_5_BIRTHS,
                      FASTEST_GROWING_ALL_BIRTHS,
                      FASTEST_DECLINING_ALL_BIRTHS,
                      FASTEST_GROWING_2_DEATHS,
                      FASTEST_DECLINING_2_DEATHS,
                      FASTEST_GROWING_3_DEATHS,
                      FASTEST_DECLINING_3_DEATHS,
                      FASTEST_GROWING_5_DEATHS,
                      FASTEST_DECLINING_5_DEATHS,
                      FASTEST_GROWING_ALL_DEATHS,
                      FASTEST_DECLINING_ALL_DEATHS
                      ]



    # SAVE TO EXCEL
    if export:
        report_name = "MLG MSA Analysis {DATETIME}.xlsx".format(DATETIME=datetime.now().strftime("%m.%d.%Y-%H%M%S"))
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "reports", report_name)
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        df_relative_analysis.to_excel(writer, sheet_name="relative_CBSA_analysis")
        df_abs_analysis.to_excel(writer, sheet_name="absolute_CBSA_analysis")
        df_abs_states.to_excel(writer, sheet_name="state_aggregation")
        df_all_rankings.to_excel(writer, sheet_name="Rankings")
        df_consolidated_rankings.to_excel(writer, sheet_name="Consolidated Rankings")
        write_multiple_dfs(writer, all_df_reports, 'MLG MSA Tables', 1)
        writer.save()

    if plot:
        import plotly.express as px
        state_list = mappings.states_df["state"].values.tolist()
        graphs_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "reports", "graphs", "states")
        df_population_growth_graphs = df_relative_analysis.iloc[:, 0:9]
        df_population_growth_graphs["STATE"] = df_relative_analysis["STATE"]
        # df_population_growth_graphs = df_population_growth_graphs.reset_index()
        for state in state_list:
            p = os.path.join(graphs_path, state + ".png")
            to_graph_df = df_population_growth_graphs[df_population_growth_graphs["STATE"] == state]
            if not to_graph_df.empty:
                del to_graph_df["STATE"]
                to_graph_df.columns = list(range(2011, 2020))
                to_graph_df = to_graph_df.stack().reset_index()
                to_graph_df.columns = ["CBSA", "Year", "% Change"]
                fig = px.line(to_graph_df, x='Year', y="% Change", color="CBSA")
                fig.write_image(p)


def main():
    costar_mf = CostarMF("costar_mf_all_cbsa_train.xlsx")
    cb = CensusBureau()
    analyze_population(cb, costar_mf, export=1, plot=0)


if __name__ == "__main__":
    main()
