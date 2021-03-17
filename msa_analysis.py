from CensusBureau import CensusBureau
from DataScraper import Mappings
from BureauLaborStatistics import BLS, all_counties_national_columns
import pandas as pd
import os
from datetime import datetime
from DataScraper import CensusBureauPermits
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
    pass


def prep_bls_data(bls):
    multi_industry_bls = bls.df[bls.df["industry_code"] == '10']  # filter for all industries
    multi_industry_bls = multi_industry_bls[multi_industry_bls["own_code"] == '0'] #  filter for total covered
    multi_industry_bls = multi_industry_bls[~multi_industry_bls["agglvl_code"].isnull()]  # filter out Null data
    multi_industry_bls["agglvl_code"] = multi_industry_bls["agglvl_code"].astype(int)
    multi_industry_bls = multi_industry_bls.replace({'own_code': lvl_map})  # map ownership codes
    return multi_industry_bls


def analyze_population(export, plot):
    """
    Algorithm for analyzing population data in the US

    DATA DOCUMENTATION:
    https://www.census.gov/programs-surveys/popest/about/glossary.html
    https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cbsa-est2019-alldata.pdf
    :param cb:
    :return:
    """

    population_threshold = 0
    top_ranking_threshold = .10
    bottom_ranking_threshold = .10

    cb_permits = CensusBureauPermits()

    max_year = cb_permits.permit_data['period'].max().year - 1
    min_year = max_year - 5
    grouped_permits_by_year = cb_permits.permit_data.groupby(['CBSA', cb_permits.permit_data['period'].dt.year]).agg("sum")

    max_year_permits = grouped_permits_by_year[grouped_permits_by_year.index.get_level_values('period')
                                               == grouped_permits_by_year.index.get_level_values('period').max()]
    max_year_permits.reset_index(inplace=True)
    max_year_permits.set_index("CBSA", inplace=True)

    find_average_permits_5_years = grouped_permits_by_year.reset_index()
    find_average_permits_5_years = find_average_permits_5_years[(find_average_permits_5_years['period'] >= min_year) & (find_average_permits_5_years['period'] <= max_year)]
    average_permits_5_years = find_average_permits_5_years.groupby(['CBSA']).agg("mean")

    # GET THE CB DATA
    cb = CensusBureau()
    features = cb.pop_features
    cb_pop_data_master = cb.fetch_county_population_csv()
    cb.build_acs_report(years=list(range(2019, 2020)), export=False)

    # Filter for state level population data
    df_master_states = cb_pop_data_master[cb_pop_data_master["COUNTY"] == '000']

    # Remove states from county level list
    cb_pop_data_master = cb_pop_data_master[cb_pop_data_master["COUNTY"] != '000']
    filtered_cb_pop_data = cb_pop_data_master[cb_pop_data_master["POPESTIMATE2019"] >= population_threshold]

    # GET AND PREP THE BLS DATA
    bls = BLS()

    bls.bls_industry_data(years=list(range(2019, 2020)), filter=all_counties_national_columns,
                          export=False)
    multi_industry_bls = prep_bls_data(bls)

    s1 = set(filtered_cb_pop_data["COMBINED_COUNTY_ID"].values.tolist())
    s2 = set(multi_industry_bls["area_fips"].values.tolist())
    remove = s2 - s1
    multi_industry_bls = multi_industry_bls[~multi_industry_bls["area_fips"].isin(remove) & ~pd.isnull(multi_industry_bls["area_fips"])]
    filtered_cb_pop_data.columns = [i.replace("_", "") for i in filtered_cb_pop_data.columns.tolist()]  #  only use this on County level data, not CBSA
    filtered_cb_pop_data = filtered_cb_pop_data.set_index("COMBINEDCOUNTYID", drop=False)

    # pd.qcut(filtered_cb_pop_data["POPESTIMATE2019"], 10, labels=False)

    multi_industry_bls["MSA"] = multi_industry_bls["area_fips"].map(mappings.cbsa_fips_df["MSA Title"])
    multi_industry_bls["CBSA Id"] = multi_industry_bls["area_fips"].map(mappings.cbsa_fips_df['CBSA Id'])
    multi_industry_bls["State"] = multi_industry_bls["area_fips"].map(mappings.cbsa_fips_df["State"])
    multi_industry_bls["County"] = multi_industry_bls["area_fips"].map(mappings.cbsa_fips_df["County Title"])

    cols = list(multi_industry_bls)
    cols.insert(0, cols.pop(cols.index('County')))
    cols.insert(0, cols.pop(cols.index('MSA')))
    cols.insert(0, cols.pop(cols.index('State')))
    multi_industry_bls = multi_industry_bls.ix[:, cols]

    multi_industry_bls.set_index(['area_fips', 'year'], inplace=True)
    cb.cb_report.set_index(["combined_county_id", 'year'], inplace=True, drop=False)



    # cbsa_ids = merged['CBSA Id']
    #
    # for cbsa_id in cbsa_ids:
    #     curr_df = merged[merged['CBSA Id'] == cbsa_ids]
    #     curr_df['Median Household Income'].product

    # todo: add inflows/migration 3, 5, 10 yr change




    # temp = merged.assign(col=merged['Median Household Income'] * merged['Total Households']).groupby('CBSA Id',
    #                                                                                           as_index=False).col.sum()
    # temp.set_index('CBSA Id', inplace=True)
    # total_households_by_cbsa = merged[['CBSA Id', 'Total Households']].groupby('CBSA Id').agg('sum')
    # median_hhi_by_cbsa = temp['col'] / total_households_by_cbsa['Total Households']

    print



    """
    -------------------------------------------START RELATIVE ANALYSIS
    """

    df_relative_analysis = pd.DataFrame(index=filtered_cb_pop_data.index)

    # COMPUTE ANNUAL PERCENT CHANGE
    df_relative_analysis["POPULATION_%CHG_2011"] = filtered_cb_pop_data["POPESTIMATE2011"] / filtered_cb_pop_data["POPESTIMATE2010"] - 1
    df_relative_analysis["POPULATION_%CHG_2012"] = filtered_cb_pop_data["POPESTIMATE2012"] / filtered_cb_pop_data["POPESTIMATE2011"] - 1
    df_relative_analysis["POPULATION_%CHG_2013"] = filtered_cb_pop_data["POPESTIMATE2013"] / filtered_cb_pop_data["POPESTIMATE2012"] - 1
    df_relative_analysis["POPULATION_%CHG_2014"] = filtered_cb_pop_data["POPESTIMATE2014"] / filtered_cb_pop_data["POPESTIMATE2013"] - 1
    df_relative_analysis["POPULATION_%CHG_2015"] = filtered_cb_pop_data["POPESTIMATE2015"] / filtered_cb_pop_data["POPESTIMATE2014"] - 1
    df_relative_analysis["POPULATION_%CHG_2016"] = filtered_cb_pop_data["POPESTIMATE2016"] / filtered_cb_pop_data["POPESTIMATE2015"] - 1
    df_relative_analysis["POPULATION_%CHG_2017"] = filtered_cb_pop_data["POPESTIMATE2017"] / filtered_cb_pop_data["POPESTIMATE2016"] - 1
    df_relative_analysis["POPULATION_%CHG_2018"] = filtered_cb_pop_data["POPESTIMATE2018"] / filtered_cb_pop_data["POPESTIMATE2017"] - 1
    df_relative_analysis["POPULATION_%CHG_2019"] = filtered_cb_pop_data["POPESTIMATE2019"] / filtered_cb_pop_data["POPESTIMATE2018"] - 1

    # LOOK AT ROC OF ANNUAL GROWTH
    df_relative_analysis["NPOPCHG2011_%CHG"] = filtered_cb_pop_data["NPOPCHG2011"] / filtered_cb_pop_data["NPOPCHG2010"] - 1
    df_relative_analysis["NPOPCHG2012_%CHG"] = filtered_cb_pop_data["NPOPCHG2012"] / filtered_cb_pop_data["NPOPCHG2011"] - 1
    df_relative_analysis["NPOPCHG2013_%CHG"] = filtered_cb_pop_data["NPOPCHG2013"] / filtered_cb_pop_data["NPOPCHG2012"] - 1
    df_relative_analysis["NPOPCHG2014_%CHG"] = filtered_cb_pop_data["NPOPCHG2014"] / filtered_cb_pop_data["NPOPCHG2013"] - 1
    df_relative_analysis["NPOPCHG2015_%CHG"] = filtered_cb_pop_data["NPOPCHG2015"] / filtered_cb_pop_data["NPOPCHG2014"] - 1
    df_relative_analysis["NPOPCHG2016_%CHG"] = filtered_cb_pop_data["NPOPCHG2016"] / filtered_cb_pop_data["NPOPCHG2015"] - 1
    df_relative_analysis["NPOPCHG2017_%CHG"] = filtered_cb_pop_data["NPOPCHG2017"] / filtered_cb_pop_data["NPOPCHG2016"] - 1
    df_relative_analysis["NPOPCHG2018_%CHG"] = filtered_cb_pop_data["NPOPCHG2018"] / filtered_cb_pop_data["NPOPCHG2017"] - 1
    df_relative_analysis["NPOPCHG2019_%CHG"] = filtered_cb_pop_data["NPOPCHG2019"] / filtered_cb_pop_data["NPOPCHG2018"] - 1

    # COMPUTE POPULATION AVERAGES: 2, 3, 5, ALL
    _2_YEAR_POP_SLICE = features[15:17]
    _3_YEAR_POP_SLICE = features[14:17]
    _5_YEAR_POP_SLICE = features[12:17]
    _ALL_YEAR_POP_SLICE = features[7:17]
    _2_YEAR_POP_AVG = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_POP_SLICE)].mean(axis=1)
    _3_YEAR_POP_AVG = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_POP_SLICE)].mean(axis=1)
    _5_YEAR_POP_AVG = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_POP_SLICE)].mean(axis=1)
    _ALL_YEAR_POP_AVG = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_POP_SLICE)].mean(axis=1)

    # 2 AND 3 YEAR AND 5 YEAR POPULATION CHANGE AVERAGES
    _2_YEAR_CHANGE_SLICE = features[25:27]
    _3_YEAR_CHANGE_SLICE = features[24:27]
    _5_YEAR_CHANGE_SLICE = features[22:27]
    _ALL_YEAR_CHANGE_SLICE = features[17:27]
    df_relative_analysis["_2_YEAR_POPULATION_RATE_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["_3_YEAR_POPULATION_RATE_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["_5_YEAR_POPULATION_RATE_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["_ALL_YEAR_POPULATION_RATE_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_CHANGE_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # DOMESTIC MIGRATION
    _2_YEAR_DOMESTIC_MIGRATION = features[75:77]
    _3_YEAR_DOMESTIC_MIGRATION = features[74:77]
    _5_YEAR_DOMESTIC_MIGRATION = features[72:77]
    _ALL_YEAR_DOMESTIC_MIGRATION = features[67:77]
    df_relative_analysis["2_YEAR_DOMESTIC_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DOMESTIC_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DOMESTIC_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DOMESTIC_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # INTERNATIONAL MIGRATION
    _2_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[65:67]
    _3_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[64:67]
    _5_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[62:67]
    _ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[57:67]
    df_relative_analysis["2_YEAR_INTERNATIONAL_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(
        _2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_INTERNATIONAL_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(
        _3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_INTERNATIONAL_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(
        _5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_INTERNATIONAL_MIGRATION"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(
        _ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # MOST DEATHS 2, 3, 5, ALL YEARS
    _2_YEAR_DEATHS_SLICE = features[45:47]
    _3_YEAR_DEATHS_SLICE = features[44:47]
    _5_YEAR_DEATHS_SLICE = features[42:47]
    _ALL_YEAR_DEATHS_SLICE = features[37:47]
    df_relative_analysis["2_YEAR_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_DEATHS_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # MOST BIRTHS 2, 3, 5, ALL YEARS
    _2_YEAR_BIRTHS_SLICE = features[35:37]
    _3_YEAR_BIRTHS_SLICE = features[34:37]
    _5_YEAR_BIRTHS_SLICE = features[32:37]
    _ALL_YEAR_BIRTHS_SLICE = features[27:37]
    df_relative_analysis["2_YEAR_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_BIRTHS_SLICE)].mean(
        axis=1) / _ALL_YEAR_POP_AVG

    # PERCENT OF NATURAL INCREASE FROM BIRTHS OR DEATHS
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2011"] = filtered_cb_pop_data["BIRTHS2011"] / filtered_cb_pop_data["NATURALINC2010"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2012"] = filtered_cb_pop_data["BIRTHS2012"] / filtered_cb_pop_data["NATURALINC2011"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2013"] = filtered_cb_pop_data["BIRTHS2013"] / filtered_cb_pop_data["NATURALINC2012"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2014"] = filtered_cb_pop_data["BIRTHS2014"] / filtered_cb_pop_data["NATURALINC2013"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2015"] = filtered_cb_pop_data["BIRTHS2015"] / filtered_cb_pop_data["NATURALINC2014"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2016"] = filtered_cb_pop_data["BIRTHS2016"] / filtered_cb_pop_data["NATURALINC2015"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2017"] = filtered_cb_pop_data["BIRTHS2017"] / filtered_cb_pop_data["NATURALINC2016"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2018"] = filtered_cb_pop_data["BIRTHS2018"] / filtered_cb_pop_data["NATURALINC2017"] - 1
    df_relative_analysis["PER_BIRTHS_NAT_MIG_2019"] = filtered_cb_pop_data["BIRTHS2019"] / filtered_cb_pop_data["NATURALINC2018"] - 1

    df_relative_analysis["PER_DEATHS_NAT_MIG_2011"] = filtered_cb_pop_data["DEATHS2011"] / filtered_cb_pop_data["NATURALINC2010"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2012"] = filtered_cb_pop_data["DEATHS2012"] / filtered_cb_pop_data["NATURALINC2011"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2013"] = filtered_cb_pop_data["DEATHS2013"] / filtered_cb_pop_data["NATURALINC2012"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2014"] = filtered_cb_pop_data["DEATHS2014"] / filtered_cb_pop_data["NATURALINC2013"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2015"] = filtered_cb_pop_data["DEATHS2015"] / filtered_cb_pop_data["NATURALINC2014"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2016"] = filtered_cb_pop_data["DEATHS2016"] / filtered_cb_pop_data["NATURALINC2015"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2017"] = filtered_cb_pop_data["DEATHS2017"] / filtered_cb_pop_data["NATURALINC2016"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2018"] = filtered_cb_pop_data["DEATHS2018"] / filtered_cb_pop_data["NATURALINC2017"] - 1
    df_relative_analysis["PER_DEATHS_NAT_MIG_2019"] = filtered_cb_pop_data["DEATHS2019"] / filtered_cb_pop_data["NATURALINC2018"] - 1

    # PERCENT OF NET MIGRATION FROM DOMESTIC AND INTERNATIONAL
    df_relative_analysis["PER_DOMESTIC_MIG_2011"] = filtered_cb_pop_data["DOMESTICMIG2011"] / filtered_cb_pop_data["NETMIG2010"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2012"] = filtered_cb_pop_data["DOMESTICMIG2012"] / filtered_cb_pop_data["NETMIG2011"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2013"] = filtered_cb_pop_data["DOMESTICMIG2013"] / filtered_cb_pop_data["NETMIG2012"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2014"] = filtered_cb_pop_data["DOMESTICMIG2014"] / filtered_cb_pop_data["NETMIG2013"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2015"] = filtered_cb_pop_data["DOMESTICMIG2015"] / filtered_cb_pop_data["NETMIG2014"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2016"] = filtered_cb_pop_data["DOMESTICMIG2016"] / filtered_cb_pop_data["NETMIG2015"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2017"] = filtered_cb_pop_data["DOMESTICMIG2017"] / filtered_cb_pop_data["NETMIG2016"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2018"] = filtered_cb_pop_data["DOMESTICMIG2018"] / filtered_cb_pop_data["NETMIG2017"] - 1
    df_relative_analysis["PER_DOMESTIC_MIG_2019"] = filtered_cb_pop_data["DOMESTICMIG2019"] / filtered_cb_pop_data["NETMIG2018"] - 1

    df_relative_analysis["PER_INTERNATIONAL_MIG_2011"] = filtered_cb_pop_data["INTERNATIONALMIG2011"] / filtered_cb_pop_data["NETMIG2010"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2012"] = filtered_cb_pop_data["INTERNATIONALMIG2012"] / filtered_cb_pop_data["NETMIG2011"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2013"] = filtered_cb_pop_data["INTERNATIONALMIG2013"] / filtered_cb_pop_data["NETMIG2012"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2014"] = filtered_cb_pop_data["INTERNATIONALMIG2014"] / filtered_cb_pop_data["NETMIG2013"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2015"] = filtered_cb_pop_data["INTERNATIONALMIG2015"] / filtered_cb_pop_data["NETMIG2014"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2016"] = filtered_cb_pop_data["INTERNATIONALMIG2016"] / filtered_cb_pop_data["NETMIG2015"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2017"] = filtered_cb_pop_data["INTERNATIONALMIG2017"] / filtered_cb_pop_data["NETMIG2016"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2018"] = filtered_cb_pop_data["INTERNATIONALMIG2018"] / filtered_cb_pop_data["NETMIG2017"] - 1
    df_relative_analysis["PER_INTERNATIONAL_MIG_2019"] = filtered_cb_pop_data["INTERNATIONALMIG2019"] / filtered_cb_pop_data["NETMIG2018"] - 1

    """
    START ABSOLUTE ANALYSIS
    """

    df_abs_analysis = pd.DataFrame(index=filtered_cb_pop_data.index)

    # AGGREGATE ABSOLUTES: 1, 3, 5, ALL YEARS
    df_abs_analysis["1_YEAR_ABS_CHANGE"] = filtered_cb_pop_data["NPOPCHG2019"]
    df_abs_analysis["2_YEAR_ABS_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_CHANGE_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_CHANGE"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_CHANGE_SLICE)].sum(axis=1)

    # BIRTHS
    df_abs_analysis["1_YEAR_ABS_BIRTHS"] = filtered_cb_pop_data["BIRTHS2019"]
    df_abs_analysis["3_YEAR_ABS_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_BIRTHS_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_BIRTHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_BIRTHS_SLICE)].sum(axis=1)

    # DEATHS
    df_abs_analysis["1_YEAR_ABS_DEATHS"] = filtered_cb_pop_data["DEATHS2019"]
    df_abs_analysis["2_YEAR_ABS_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_DEATHS_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DEATHS"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_DEATHS_SLICE)].sum(axis=1)

    # DOMESTIC
    df_abs_analysis["1_YEAR_ABS_DOMESTIC"] = filtered_cb_pop_data["DOMESTICMIG2019"]
    df_abs_analysis["2_YEAR_ABS_DOMESTIC"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_2_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_DOMESTIC"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DOMESTIC"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DOMESTIC"] = filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_ALL_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)

    # INTERNATIONAL
    df_abs_analysis["1_YEAR_ABS_INTERNATIONAL"] = filtered_cb_pop_data["INTERNATIONALMIG2019"]
    df_abs_analysis["2_YEAR_ABS_INTERNATIONAL"] = filtered_cb_pop_data[
        filtered_cb_pop_data.columns.intersection(_2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_INTERNATIONAL"] = filtered_cb_pop_data[
        filtered_cb_pop_data.columns.intersection(_3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"] = filtered_cb_pop_data[
        filtered_cb_pop_data.columns.intersection(_5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_INTERNATIONAL"] = filtered_cb_pop_data[
        filtered_cb_pop_data.columns.intersection(_ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)



    # ASSIGN RANKINGS
    # DO RELATIVE FIRST
    df_all_rankings = pd.DataFrame(index=filtered_cb_pop_data.index)
    for col in df_relative_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_relative_analysis[col], 10, labels=False)

    for col in df_abs_analysis.columns[:-2]:
        col_name = col + "_RANK"
        df_all_rankings[col_name] = pd.qcut(df_abs_analysis[col], 10, labels=False)

    # ASSIGN RANKINGS FIRST BEFORE ADDING TEXT COLUMNS
    df_abs_analysis["County"] = mappings.cbsa_fips_df["County Title"]
    df_abs_analysis["MSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_abs_analysis["State"] = mappings.cbsa_fips_df["State"]
    cols = list(df_abs_analysis)
    cols.insert(0, cols.pop(cols.index('County')))
    cols.insert(0, cols.pop(cols.index('MSA')))
    cols.insert(0, cols.pop(cols.index('State')))
    df_abs_analysis = df_abs_analysis.ix[:, cols]

    df_relative_analysis["County"] = mappings.cbsa_fips_df["County Title"]
    df_relative_analysis["MSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_relative_analysis["State"] = mappings.cbsa_fips_df["State"]
    cols = list(df_relative_analysis)
    cols.insert(0, cols.pop(cols.index('County')))
    cols.insert(0, cols.pop(cols.index('MSA')))
    cols.insert(0, cols.pop(cols.index('State')))
    df_relative_analysis = df_relative_analysis.ix[:, cols]

    # BUILD CONSOLIDATED RANKINGS BASED ON RELATIVE CHANGES IN DATA
    df_all_rankings["County"] = mappings.cbsa_fips_df["County Title"]
    df_all_rankings["MSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_all_rankings["State"] = mappings.cbsa_fips_df["State"]
    cols = list(df_all_rankings)
    cols.insert(0, cols.pop(cols.index('County')))
    cols.insert(0, cols.pop(cols.index('MSA')))
    cols.insert(0, cols.pop(cols.index('State')))
    df_all_rankings = df_all_rankings.ix[:, cols]

    df_consolidated_rankings = pd.DataFrame(index=filtered_cb_pop_data.index)
    df_consolidated_rankings["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]

    df_relative_analysis["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]
    df_abs_analysis["Assigned CBSA"] = mappings.cbsa_fips_df["MSA Title"]

    df_consolidated_rankings["2019 Pop Estimate"] = filtered_cb_pop_data["POPESTIMATE2019"]
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

    # RANKINGS & REPORTS
    # GET FASTEST GROWING/DECLINING YOY
    FASTEST_GROWING_YOY = df_relative_analysis.sort_values(by=["POPULATION_%CHG_2019"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "POPULATION_%CHG_2019"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_YOY = df_relative_analysis.sort_values(by=["POPULATION_%CHG_2019"], ascending=True).head(
        round(len(df_relative_analysis) * bottom_ranking_threshold))[["MSA", "POPULATION_%CHG_2019"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_2_YEAR = \
    df_relative_analysis.sort_values(by=["_2_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "_2_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_YEAR = \
    df_relative_analysis.sort_values(by=["_2_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * bottom_ranking_threshold))[["MSA", "_2_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_YEAR = \
    df_relative_analysis.sort_values(by=["_3_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "_3_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_YEAR = \
    df_relative_analysis.sort_values(by=["_3_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * bottom_ranking_threshold))[["MSA", "_3_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_YEAR = \
    df_relative_analysis.sort_values(by=["_5_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "_5_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_YEAR = \
    df_relative_analysis.sort_values(by=["_5_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * bottom_ranking_threshold))[["MSA", "_5_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_YEAR = \
    df_relative_analysis.sort_values(by=["_ALL_YEAR_POPULATION_RATE_CHANGE"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "_ALL_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_YEAR = \
    df_relative_analysis.sort_values(by=["_ALL_YEAR_POPULATION_RATE_CHANGE"], ascending=True).head(
        round(len(df_relative_analysis) * bottom_ranking_threshold))[["MSA", "_ALL_YEAR_POPULATION_RATE_CHANGE"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])

    # BUILD DOMESTIC TABLES
    FASTEST_GROWING_2_DOMESTIC = \
    df_relative_analysis.sort_values(by=["2_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_DOMESTIC = \
    df_relative_analysis.sort_values(by=["2_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_DOMESTIC = \
    df_relative_analysis.sort_values(by=["3_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_DOMESTIC = \
    df_relative_analysis.sort_values(by=["3_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_DOMESTIC = \
    df_relative_analysis.sort_values(by=["5_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_DOMESTIC = \
    df_relative_analysis.sort_values(by=["5_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_DOMESTIC = \
    df_relative_analysis.sort_values(by=["ALL_YEAR_DOMESTIC_MIGRATION"], ascending=False).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_DOMESTIC = \
    df_relative_analysis.sort_values(by=["ALL_YEAR_DOMESTIC_MIGRATION"], ascending=True).head(
        round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_DOMESTIC_MIGRATION"]].join(
        filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])

    FASTEST_GROWING_2_INT = \
        df_relative_analysis.sort_values(by=["2_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_INT = \
        df_relative_analysis.sort_values(by=["2_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_INT = \
        df_relative_analysis.sort_values(by=["3_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_INT = \
        df_relative_analysis.sort_values(by=["3_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_INT = \
        df_relative_analysis.sort_values(by=["5_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_INT = \
        df_relative_analysis.sort_values(by=["5_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_INT = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_INTERNATIONAL_MIGRATION"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_INT = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_INTERNATIONAL_MIGRATION"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_INTERNATIONAL_MIGRATION"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])

    # BUILD BIRTHS AND DEATHS
    FASTEST_GROWING_2_BIRTHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_BIRTHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_BIRTHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_BIRTHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_BIRTHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_BIRTHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_BIRTHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_BIRTHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_BIRTHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_BIRTHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_BIRTHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])

    FASTEST_GROWING_2_DEATHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_2_DEATHS = \
        df_relative_analysis.sort_values(by=["2_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "2_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_3_DEATHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_3_DEATHS = \
        df_relative_analysis.sort_values(by=["3_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "3_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_5_DEATHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_5_DEATHS = \
        df_relative_analysis.sort_values(by=["5_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "5_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_GROWING_ALL_DEATHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_DEATHS"], ascending=False).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])
    FASTEST_DECLINING_ALL_DEATHS = \
        df_relative_analysis.sort_values(by=["ALL_YEAR_DEATHS"], ascending=True).head(
            round(len(df_relative_analysis) * top_ranking_threshold))[["MSA", "ALL_YEAR_DEATHS"]].join(
            filtered_cb_pop_data[["POPESTIMATE2019", "NPOPCHG2019"]])

    # TODO: MAP OD DEATHS TO CBSA TO GAUGE CONTRIBUTION TO DEATH RATES

    # all_df_reports = [FASTEST_GROWING_YOY,
    #                   FASTEST_DECLINING_YOY,
    #                   FASTEST_GROWING_2_YEAR,
    #                   FASTEST_DECLINING_2_YEAR,
    #                   FASTEST_GROWING_3_YEAR,
    #                   FASTEST_DECLINING_3_YEAR,
    #                   FASTEST_GROWING_5_YEAR,
    #                   FASTEST_DECLINING_5_YEAR,
    #                   FASTEST_GROWING_ALL_YEAR,
    #                   FASTEST_DECLINING_ALL_YEAR,
    #                   FASTEST_GROWING_2_DOMESTIC,
    #                   FASTEST_DECLINING_2_DOMESTIC,
    #                   FASTEST_GROWING_3_DOMESTIC,
    #                   FASTEST_DECLINING_3_DOMESTIC,
    #                   FASTEST_GROWING_5_DOMESTIC,
    #                   FASTEST_DECLINING_5_DOMESTIC,
    #                   FASTEST_GROWING_ALL_DOMESTIC,
    #                   FASTEST_DECLINING_ALL_DOMESTIC,
    #                   FASTEST_GROWING_2_INT,
    #                   FASTEST_DECLINING_2_INT,
    #                   FASTEST_GROWING_3_INT,
    #                   FASTEST_DECLINING_3_INT,
    #                   FASTEST_GROWING_5_INT,
    #                   FASTEST_DECLINING_5_INT,
    #                   FASTEST_GROWING_ALL_INT,
    #                   FASTEST_DECLINING_ALL_INT,
    #                   FASTEST_GROWING_2_BIRTHS,
    #                   FASTEST_DECLINING_2_BIRTHS,
    #                   FASTEST_GROWING_3_BIRTHS,
    #                   FASTEST_DECLINING_3_BIRTHS,
    #                   FASTEST_GROWING_5_BIRTHS,
    #                   FASTEST_DECLINING_5_BIRTHS,
    #                   FASTEST_GROWING_ALL_BIRTHS,
    #                   FASTEST_DECLINING_ALL_BIRTHS,
    #                   FASTEST_GROWING_2_DEATHS,
    #                   FASTEST_DECLINING_2_DEATHS,
    #                   FASTEST_GROWING_3_DEATHS,
    #                   FASTEST_DECLINING_3_DEATHS,
    #                   FASTEST_GROWING_5_DEATHS,
    #                   FASTEST_DECLINING_5_DEATHS,
    #                   FASTEST_GROWING_ALL_DEATHS,
    #                   FASTEST_DECLINING_ALL_DEATHS
    #                   ]

    #df_relative_analysis
    #df_abs_analysis

    merged = pd.concat([cb.cb_report, multi_industry_bls], axis=1)
    merged = merged[merged['CBSA Id'].notna()]
    # merged = merged.merge(max_year_permits, left_on='CBSA Id', right_on='CBSA')
    merged = merged[merged['Median Household Income'].notna()]
    merged.set_index('combined_county_id', inplace=True)

    merged["1 Year Population % Change"] = df_relative_analysis["POPULATION_%CHG_2019"]
    merged["3 Year Population % Change"]  = df_relative_analysis["_3_YEAR_POPULATION_RATE_CHANGE"]
    merged["5 Year Population % Change"] = df_relative_analysis["_5_YEAR_POPULATION_RATE_CHANGE"]
    merged["10 Year Population % Change"] = df_relative_analysis["_ALL_YEAR_POPULATION_RATE_CHANGE"]

    merged["1 Year Population Nominal Change"] = df_abs_analysis["1_YEAR_ABS_CHANGE"]
    merged["3 Year Population Nominal Change"] = df_abs_analysis["3_YEAR_ABS_CHANGE"]
    merged["5 Year Population Nominal Change"] = df_abs_analysis["5_YEAR_ABS_CHANGE"]
    merged["10 Year Population Nominal Change"] = df_abs_analysis["ALL_YEAR_ABS_CHANGE"]

    merged["2019 Net Migration"] = filtered_cb_pop_data["NETMIG2019"]
    merged["Nominal 5 Yr Net Migration"] = sum([filtered_cb_pop_data["NETMIG2019"],
                                        filtered_cb_pop_data["NETMIG2018"],
                                        filtered_cb_pop_data["NETMIG2017"],
                                        filtered_cb_pop_data["NETMIG2016"],
                                        filtered_cb_pop_data["NETMIG2015"],
                                        ])

    merged['Total 5 Year Domestic'] = df_abs_analysis["5_YEAR_ABS_DOMESTIC"]
    merged['Total 5 Year International'] = df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"]

    merged['% Net Migration 5 Year from Domestic'] = df_abs_analysis["5_YEAR_ABS_DOMESTIC"] / merged["Nominal 5 Yr Net Migration"]
    merged['% Net Migration 5 Year International'] = df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"] / merged["Nominal 5 Yr Net Migration"]
    merged['5 Year Growth from Net Migration'] = merged["Nominal 5 Yr Net Migration"] / filtered_cb_pop_data["POPESTIMATE2019"]

    # _5_YEAR_POPULATION_CHANGE_SLICE = features[21:27]

    # filtered_cb_pop_data[filtered_cb_pop_data.columns.intersection(_5_YEAR_POPULATION_CHANGE_SLICE)].sum(axis=1)


    cols = list(merged)
    cols.insert(0, cols.pop(cols.index('state')))
    cols.insert(0, cols.pop(cols.index('county')))
    cols.insert(0, cols.pop(cols.index('CBSA Id')))
    cols.insert(0, cols.pop(cols.index('State')))
    cols.insert(0, cols.pop(cols.index('MSA')))
    cols.insert(0, cols.pop(cols.index('County')))
    merged = merged.ix[:, cols]

    merged.reset_index(drop=False, inplace=True)  # drop to get combined county back

    del merged['NAME']
    del merged['own_code']
    del merged['industry_code']
    del merged['industry_code_name']
    del merged['disclosure_code']
    del merged['size_code']
    del merged['agglvl_code']
    del merged['qtr']

    merged.columns = [i.replace("_", " ") for i in merged.columns.tolist()]

    multi_industry_bls = multi_industry_bls.reset_index()

    # SAVE TO EXCEL
    if export:
        report_name = "MLG MSA Analysis {DATETIME}.xlsx".format(DATETIME=datetime.now().strftime("%m.%d.%Y-%H%M%S"))
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "reports", report_name)
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        cb_pop_data_master.to_excel(writer, "original_source_data")
        df_relative_analysis.to_excel(writer, sheet_name="relative_CBSA_analysis")
        df_abs_analysis.to_excel(writer, sheet_name="absolute_CBSA_analysis")
        df_all_rankings.to_excel(writer, sheet_name="Rankings")
        df_consolidated_rankings.to_excel(writer, sheet_name="Consolidated Rankings")
        multi_industry_bls.to_excel(writer, sheet_name="BLS Data")
        df_consolidated_rankings[df_consolidated_rankings.index.isin(mlg_msas)].to_excel(writer, sheet_name="MLG Markets Rankings")
        merged.to_excel(writer, sheet_name='dashboard')
        # write_multiple_dfs(writer, all_df_reports, 'MLG MSA Tables', 1)
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
    analyze_population(export=1, plot=0)


if __name__ == "__main__":
    main()
