from CensusBureau import CensusBureau
from DataScraper import Mappings
from DataScraper import BLS
import pandas as pd
import os
from datetime import datetime
from Costar import CostarMultifamily
import numpy as np
from collections import defaultdict

mappings = Mappings()

lvl_map = {"1": "Federal Government",
           "2": "State Government",
           "3": "Local Government",
           "5": "Private", }


def write_multiple_dfs(writer, df_list, sheets, spaces):
    col = 0
    for dataframe in df_list:
        # dataframe.iloc[:,1] = pd.Series(["{0:.2f}%".format(val * 100) for val in dataframe.iloc[:,1]], index=dataframe.index)
        dataframe.to_excel(writer, sheet_name=sheets, startrow=0, startcol=col)
        col = col + len(dataframe.columns) + spaces + 1
    writer.save()


mlg_msas = [10740,
            12060,
            16980,
            17140,
            18140,
            19100,
            19780,
            23540,
            26420,
            28140,
            33340,
            33460,
            36420,
            37980,
            38060,
            38300,
            45300,
            46140,
            ]


def replace_ranks(df):
    # function for inverting rankings
    df = df.replace(0, 9)
    df = df.replace(1, 8)
    df = df.replace(2, 7)
    df = df.replace(3, 6)
    df = df.replace(4, 5)
    df = df.replace(5, 4)
    df = df.replace(6, 3)
    df = df.replace(7, 2)
    df = df.replace(8, 1)
    df = df.replace(9, 0)
    return df


def relative_analysis():
    """

    :return:
    """


def analyze_population(cb, costar_mf, bls, export, plot):
    """
    Algorithm for analyzing population data in the US

    DATA DOCUMENTATION:
    https://www.census.gov/programs-surveys/popest/about/glossary.html
    https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cbsa-est2019-alldata.pdf
    :param cb:
    :return:
    """

    # build cbsa_msa
    # cbsa_ssa = bls.cbsa_ssa.iloc[:, [2, 3, 4]].sort_values("CBSA").values.tolist()
    # d = defaultdict(list)
    # for i in cbsa_ssa:
    #     for j in i[:2]:
    #         d[i[2]].append(str(j))
    #
    # columns_names = bls.df.iloc[:, 8:].columns
    # bls.df[columns_names] = bls.df.iloc[:, 8:].apply(pd.to_numeric, errors='ignore')
    # # filter on aggregation level code and industry code
    # all_ind_county_bls_df = bls.df[bls.df["industry_code"] == '10']
    # all_ind_county_bls_df = all_ind_county_bls_df[all_ind_county_bls_df["agglvl_code"] == '70']
    #
    # # aggregate the county level all industry cbsa data
    # cbsa_demo_data = pd.pivot_table(all_ind_county_bls_df,
    #                                 index=["area_fips", "year"],
    #                                 values=bls.df.columns[8:15],
    #                                 aggfunc=np.mean)
    # cbsa_demo_data = cbsa_demo_data.unstack()
    # master = pd.DataFrame(columns=cbsa_demo_data.columns.map('-'.join).str.strip('-'))
    # for k in d.keys():
    #     group_on = d[k]
    #     t = cbsa_demo_data[cbsa_demo_data.index.isin(group_on)].sum()
    #     df_T = pd.DataFrame(data=t).T
    #     cols = df_T.columns.map('-'.join).str.strip('-')
    #     df_T.columns = cols
    #     df_T["CBSA"] = int(k)
    #     df_T["FIPS/SSA List"] = str(group_on)
    #     master = master.append(df_T)
    # master = master.set_index("CBSA", drop=True)
    # master.index = master.index.map(int)

    # INDUSTRY REPORT
    multi_industry_bls = bls.df[bls.df["industry_code"] != '10']
    # multi_industry_bls_counties_only = multi_industry_bls[multi_industry_bls["agglvl_code"] == '72'].sort_values(
    # "annual_avg_emplvl", ascending=False)
    # multi_industry_bls["industry_code"] = multi_industry_bls["industry_code"].map(bls.cew_codes_mapping)
    multi_industry_bls = multi_industry_bls[~multi_industry_bls["agglvl_code"].isnull()]
    multi_industry_bls["agglvl_code"] = multi_industry_bls["agglvl_code"].astype(int)
    multi_industry_bls = multi_industry_bls.replace({'own_code': lvl_map})  # map ownership codes
    # ssa_cbsa_name_map = dict(zip(bls.cbsa_ssa["FIPS"].astype(str), bls.cbsa_ssa["CBSA NAME"]))
    # multi_industry_bls["CBSA Name"] = multi_industry_bls["area_fips"].map(ssa_cbsa_name_map)

    # HARDCODED POPULATION THRESHOLD - SET TO 200,000
    POPULATION_THRESHOLD = 0
    THRESHOLD_TOP = .10
    THRESHOLD_BOTTOM = .10

    features = cb.pop_features
    df = cb.fetch_county_population_csv()
    df = df[df["POPESTIMATE2019"] >= POPULATION_THRESHOLD]
    _2019_MEDIAN = df["POPESTIMATE2019"].median()

    s1 = set(df["COMBINED_COUNTY_ID"].values.tolist())
    s2 = set(multi_industry_bls["area_fips"].values.tolist())
    remove = s2 - s1
    multi_industry_bls = multi_industry_bls[~multi_industry_bls["area_fips"].isin(remove) & ~pd.isnull(multi_industry_bls["area_fips"])]
    df.columns = [i.replace("_", "") for i in df.columns.tolist()]  # only use this on County level data, not CBSA
    df = df.set_index("COMBINEDCOUNTYID")

    pd.qcut(df["POPESTIMATE2019"], 10, labels=False)
    multi_industry_bls["MSA Title"] = multi_industry_bls["area_fips"].map(mappings.cbsa_fips_df["MSA Title"])
    multi_industry_bls = multi_industry_bls.set_index(["area_fips", "year"])

    # CBSA_NAMES = df[["CBSA", "NAME"]][(df["LSAD"] == "Metropolitan Statistical Area") | (df["LSAD"] ==
    # "Micropolitan Statistical Area")].set_index("CBSA")

    # mappings.cbsa_fips_df = mappings.cbsa_fips_df[["cbsacode", "fipsstatecode"]].drop_duplicates()
    # AGGREGATE ON CBSA CODE
    # df = df[(df["LSAD"] == "Metropolitan Statistical Area") | (df["LSAD"] == "Micropolitan Statistical Area")]
    # df = df.groupby("CBSA").sum()

    """
    START RELATIVE ANALYSIS
    """

    # df = pd.merge(df, master, left_index=True, right_index=True)
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


    # legacy cbsa code
    # PERCENT CHANGE AVG ANNUAL EMP LVL CHANGE
    # df_relative_analysis["PER_AVG_ANNUAL_EMP_LVL_2016"] = df["annual_avg_emplvl-2016"] / df[
    #     "annual_avg_emplvl-2015"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_EMP_LVL_2017"] = df["annual_avg_emplvl-2017"] / df[
    #     "annual_avg_emplvl-2016"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_EMP_LVL_2018"] = df["annual_avg_emplvl-2018"] / df[
    #     "annual_avg_emplvl-2017"] - 1
    # df_relative_analysis["AVG_ANNUAL_EMP_LVL_3_YEAR_%CHG"] = df["annual_avg_emplvl-2018"] / df[
    #     "annual_avg_emplvl-2015"] - 1
    #
    # # PERCENT CHANGE AVG WEEKLY WAGE
    # df_relative_analysis["PER_AVG_WEEKLY_WAGE_2016"] = df["annual_avg_wkly_wage-2016"] / df[
    #     "annual_avg_wkly_wage-2015"] - 1
    # df_relative_analysis["PER_AVG_WEEKLY_WAGE_2017"] = df["annual_avg_wkly_wage-2017"] / df[
    #     "annual_avg_wkly_wage-2016"] - 1
    # df_relative_analysis["PER_AVG_WEEKLY_WAGE_2018"] = df["annual_avg_wkly_wage-2018"] / df[
    #     "annual_avg_wkly_wage-2017"] - 1
    # df_relative_analysis["AVG_WEEKLY_WAGE_3_YEAR_%CHG"] = df["annual_avg_wkly_wage-2018"] / df[
    #     "annual_avg_wkly_wage-2015"] - 1
    #
    # # PERCENT CHANGE ANNUAL CONTRIBUTIONS
    # df_relative_analysis["PER_AVG_ANNUAL_CONTRIBUTIONS_2016"] = df["annual_contributions-2016"] / df[
    #     "annual_contributions-2015"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_CONTRIBUTIONS_2017"] = df["annual_contributions-2017"] / df[
    #     "annual_contributions-2016"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_CONTRIBUTIONS_2018"] = df["annual_contributions-2018"] / df[
    #     "annual_contributions-2017"] - 1
    # df_relative_analysis["AVG_ANNUAL_CONTRIBUTIONS_3_YEAR_%CHG"] = df["annual_contributions-2018"] / df[
    #     "annual_contributions-2015"] - 1
    #
    # # PERCENT CHANGE AVG ANNUAL PAY
    # df_relative_analysis["PER_AVG_ANNUAL_WAGE_2016"] = df["avg_annual_pay-2016"] / df["avg_annual_pay-2015"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_WAGE_2017"] = df["avg_annual_pay-2017"] / df["avg_annual_pay-2016"] - 1
    # df_relative_analysis["PER_AVG_ANNUAL_WAGE_2018"] = df["avg_annual_pay-2018"] / df["avg_annual_pay-2017"] - 1
    # df_relative_analysis["AVG_ANNUAL_WAGE_3_YEAR_%CHG"] = df["avg_annual_pay-2018"] / df["avg_annual_pay-2015"] - 1
    #
    # # PERCENT CHANGE TAXABLE ANNUAL WAGES
    # df_relative_analysis["PER_TAX_ANNUAL_WAGES_2016"] = df["taxable_annual_wages-2016"] / df[
    #     "taxable_annual_wages-2015"] - 1
    # df_relative_analysis["PER_TAX_ANNUAL_WAGES_2017"] = df["taxable_annual_wages-2017"] / df[
    #     "taxable_annual_wages-2016"] - 1
    # df_relative_analysis["PER_TAX_ANNUAL_WAGES_2018"] = df["taxable_annual_wages-2018"] / df[
    #     "taxable_annual_wages-2017"] - 1
    # df_relative_analysis["TAX_ANNUAL_WAGES_3_YEAR_%CHG"] = df["taxable_annual_wages-2018"] / df[
    #     "taxable_annual_wages-2015"] - 1
    #
    # # TOTAL ANNUAL WAGES
    # df_relative_analysis["PER_ANNUAL_WAGE_2016"] = df["total_annual_wages-2016"] / df["total_annual_wages-2015"] - 1
    # df_relative_analysis["PER_ANNUAL_WAGE_2017"] = df["total_annual_wages-2017"] / df["total_annual_wages-2016"] - 1
    # df_relative_analysis["PER_ANNUAL_WAGE_2018"] = df["total_annual_wages-2018"] / df["total_annual_wages-2017"] - 1
    # df_relative_analysis["TOTAL_ANNUAL_WAGE_3_YEAR_%CHG"] = df["total_annual_wages-2018"] / df["total_annual_wages-2015"] - 1


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

    df_abs_analysis["County Name"] = mappings.cbsa_fips_df["County Title"]
    df_abs_analysis["State"] = mappings.cbsa_fips_df["State"]
    df_relative_analysis["County Name"] = mappings.cbsa_fips_df["County Title"]
    df_relative_analysis["State"] = mappings.cbsa_fips_df["State"]
    df_abs_states = df_abs_analysis.groupby("State").sum()

    # ASSIGN RANKINGS
    # DO RELATIVE FIRST
    df_all_rankings = pd.DataFrame(index=df.index)
    for col in df_relative_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_relative_analysis[col], 10, labels=False)

    for col in df_abs_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_abs_analysis[col], 10, labels=False)

    # BUILD CONSOLIDATED RANKINGS BASED ON RELATIVE CHANGES IN DATA
    df_all_rankings["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_consolidated_rankings = pd.DataFrame(index=df.index)
    df_consolidated_rankings["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]

    df_relative_analysis["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_abs_states["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_abs_analysis["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]

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

    # sum_list = ["3 Year Avg Weekly Wage Growth Rank",
    #             "3 Year Avg Weekly Wage Growth Rank",
    #             "3 Year Avg Annual Contributions Growth Rank",
    #             "3 Year Avg Annual Wage Growth Rank",
    #             "3 Year Avg Taxable Annual Wages Growth Rank",
    #             "3 Year Total Annual Wage Growth Rank",
    #             ]
    # df_consolidated_rankings["3 Year Avg Weekly Wage Growth Rank"] = df_all_rankings["AVG_ANNUAL_EMP_LVL_3_YEAR_%CHG_RANK"]
    # df_consolidated_rankings["3 Year Avg Weekly Wage Growth Rank"] = df_all_rankings["AVG_WEEKLY_WAGE_3_YEAR_%CHG_RANK"]
    # df_consolidated_rankings["3 Year Avg Annual Contributions Growth Rank"] = df_all_rankings["AVG_ANNUAL_CONTRIBUTIONS_3_YEAR_%CHG_RANK"]
    # df_consolidated_rankings["3 Year Avg Annual Wage Growth Rank"] = df_all_rankings["AVG_ANNUAL_WAGE_3_YEAR_%CHG_RANK"]
    # # tax growth is potentially bad, invert ranking
    # df_consolidated_rankings["3 Year Avg Taxable Annual Wages Growth Rank"] = replace_ranks(df_all_rankings["TAX_ANNUAL_WAGES_3_YEAR_%CHG_RANK"])
    # df_consolidated_rankings["3 Year Total Annual Wage Growth Rank"] = df_all_rankings["TOTAL_ANNUAL_WAGE_3_YEAR_%CHG_RANK"]
    # df_consolidated_rankings["Total 3 Year BLS Growth Rank"] = df_consolidated_rankings[sum_list].sum(axis=1)
    #
    # df_consolidated_rankings["2018 Avg Annual Pay"] = df["avg_annual_pay-2018"]
    # df_consolidated_rankings["2018 BLS Avg Emp Lvl"] = df["annual_avg_emplvl-2018"]


    # cost stuff
    # t_cbsas = list(costar_mf.cbsa_grouped_dfs.keys())
    # median_income_map = {}
    # for cbsa in t_cbsas:
    #     _temp_df = costar_mf.cbsa_grouped_dfs[cbsa]
    #     ten_yr_pop_growth = df_relative_analysis[df_relative_analysis.index == cbsa].iloc[:, 0:9]
    #     t_df = ten_yr_pop_growth.rename(
    #         columns={x: y for x, y in zip(df_relative_analysis.columns.tolist(), list(range(2011, 2020)))}).transpose()
    #     if t_df.values.size != 0:
    #         population_growth_rent_growth_corr = np.corrcoef(t_df.values.reshape(9, ), _temp_df[(_temp_df.index > 2010) & (_temp_df.index <= 2019)][
    #             "Market Effective Rent Growth 12 Mo"].values)[1][0]
    #     else:
    #         population_growth_rent_growth_corr = ""
    #
    #     # MEDIAN HHI CORRELATION WITH RENT GROWTH
    #     median_hhi_rent_growth_corr = _temp_df[(_temp_df.index > 2010) & (_temp_df.index <= 2019)]["Median Household Income"].corr(
    #         _temp_df[(_temp_df.index > 2010) & (_temp_df.index <= 2019)]["Market Effective Rent Growth 12 Mo"])
    #
    #     cap_rate_rent_growth_corr = _temp_df[(_temp_df.index > 2010) & (_temp_df.index <= 2019)]["Cap Rate"].corr(
    #         _temp_df[(_temp_df.index > 2010) & (_temp_df.index <= 2019)]["Market Effective Rent Growth 12 Mo"])
    #
    #     _2019_Median_HHI = _temp_df["Median Household Income"].filter(like="2019").values[0]
    #     _2016_Median_HHI = _temp_df["Median Household Income"].filter(like="2016").values[0]
    #     _2014_Median_HHI = _temp_df["Median Household Income"].filter(like="2014").values[0]
    #     _2010_Median_HHI = _temp_df["Median Household Income"].filter(like="2009").values[0]
    #     median_income_map[cbsa] = {"2019 Median HHI": _2019_Median_HHI,
    #                                "2016 Median HHI": _2016_Median_HHI,
    #                                "2014 Median HHI": _2014_Median_HHI,
    #                                "2010 Median HHI": _2010_Median_HHI,
    #                                "3 Year Median Income Change": _2019_Median_HHI / _2016_Median_HHI - 1 or "",
    #                                "5 Year Median Income Change": _2019_Median_HHI / _2014_Median_HHI - 1 or "",
    #                                "10 Year Median Income Change": _2019_Median_HHI / _2010_Median_HHI - 1 or "",
    #                                "Population Growth/Rent Growth Correlation": population_growth_rent_growth_corr or "",
    #                                "Medina HHI/Rent Growth Correlation": median_hhi_rent_growth_corr or "",
    #                                "Market Cap Rate vs Rent Growth Correlation": cap_rate_rent_growth_corr or "",
    #                                }
    #
    # median_inc_map_df = pd.DataFrame.from_dict(median_income_map, orient='index')
    # df_consolidated_rankings = df_consolidated_rankings.join(median_inc_map_df)

    # RANKINGS & REPORTS

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

    multi_industry_bls = multi_industry_bls.reset_index()

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
        multi_industry_bls.to_excel(writer, sheet_name="BLS Data")
        df_consolidated_rankings[df_consolidated_rankings.index.isin(mlg_msas)].to_excel(writer, sheet_name="MLG Markets Rankings")

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
    bls = BLS()
    bls.bls_industry_data()
    # costar_mf = CostarMF("costar_mf_all_cbsa.xlsx")
    costar_mf = []
    cb = CensusBureau()
    analyze_population(cb, costar_mf, bls, export=1, plot=0)


if __name__ == "__main__":
    main()
