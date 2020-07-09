from DataScraper import CensusBureau
from DataScraper import Mappings
import pandas as pd
import os

mappings = Mappings()


def analyze_population(cb):
    """
    Algorithm for analyzing population data in the US

    DATA DOCUMENTATION:
    https://www.census.gov/programs-surveys/popest/about/glossary.html
    https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cbsa-est2019-alldata.pdf
    :param cb:
    :return:
    """

    features = cb.pop_features
    df = cb.fetch_population_csv()
    CBSA_NAMES = df[["CBSA", "NAME"]][(df["LSAD"] == "Metropolitan Statistical Area") | (df["LSAD"] == "Micropolitan Statistical Area")].set_index("CBSA")
    mappings.cbsa_fips_df = mappings.cbsa_fips_df[["cbsacode", "fipsstatecode"]].drop_duplicates()
    # AGGREGATE ON CBSA CODE
    df = df.groupby("CBSA").sum()
    # MAP MSA NAME
    df["CBSA NAME"] = CBSA_NAMES

    """
    START RELATIVE ANALYSIS
    """
    
    df_relative_analysis = pd.DataFrame(index=df.index)

    # COMPUTE ANNUAL PERCENT CHANGE
    df_relative_analysis["POPESTIMATE2011_%CHG"] = df["POPESTIMATE2011"] / df["POPESTIMATE2010"] - 1
    df_relative_analysis["POPESTIMATE2012_%CHG"] = df["POPESTIMATE2012"] / df["POPESTIMATE2011"] - 1
    df_relative_analysis["POPESTIMATE2013_%CHG"] = df["POPESTIMATE2013"] / df["POPESTIMATE2012"] - 1
    df_relative_analysis["POPESTIMATE2014_%CHG"] = df["POPESTIMATE2014"] / df["POPESTIMATE2013"] - 1
    df_relative_analysis["POPESTIMATE2015_%CHG"] = df["POPESTIMATE2015"] / df["POPESTIMATE2014"] - 1
    df_relative_analysis["POPESTIMATE2016_%CHG"] = df["POPESTIMATE2016"] / df["POPESTIMATE2015"] - 1
    df_relative_analysis["POPESTIMATE2017_%CHG"] = df["POPESTIMATE2017"] / df["POPESTIMATE2016"] - 1
    df_relative_analysis["POPESTIMATE2018_%CHG"] = df["POPESTIMATE2018"] / df["POPESTIMATE2017"] - 1
    df_relative_analysis["POPESTIMATE2019_%CHG"] = df["POPESTIMATE2019"] / df["POPESTIMATE2018"] - 1

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

    # 2 AND 3 YEAR AND 5 YEAR POPULATION CHANGE AVERAGES
    _2_YEAR_CHANGE_SLICE = features[25:27]
    _3_YEAR_CHANGE_SLICE = features[24:27]
    _5_YEAR_CHANGE_SLICE = features[22:27]
    _ALL_YEAR_CHANGE_SLICE = features[17:27]
    _2_YEAR_CHANGE = df[df.columns.intersection(_2_YEAR_CHANGE_SLICE)].mean(axis=1)
    _3_YEAR_CHANGE = df[df.columns.intersection(_3_YEAR_CHANGE_SLICE)].mean(axis=1)
    _5_YEAR_CHANGE = df[df.columns.intersection(_5_YEAR_CHANGE_SLICE)].mean(axis=1)

    # COMPUTE POPULATION AVERAGES: 2, 3, 5, ALL
    _2_YEAR_POP_SLICE = features[15:17]
    _3_YEAR_POP_SLICE = features[14:17]
    _5_YEAR_POP_SLICE = features[12:17]
    _ALL_YEAR_POP_SLICE = features[7:17]
    _2_YEAR_POP_AVG = df[df.columns.intersection(_2_YEAR_POP_SLICE)].mean(axis=1)
    _3_YEAR_POP_AVG = df[df.columns.intersection(_3_YEAR_POP_SLICE)].mean(axis=1)
    _5_YEAR_POP_AVG = df[df.columns.intersection(_5_YEAR_POP_SLICE)].mean(axis=1)
    _ALL_YEAR_POP_AVG = df[df.columns.intersection(_ALL_YEAR_POP_SLICE)].mean(axis=1)

    # DOMESTIC MIGRATION
    _2_YEAR_DOMESTIC_MIGRATION = features[75:77]
    _3_YEAR_DOMESTIC_MIGRATION = features[74:77]
    _5_YEAR_DOMESTIC_MIGRATION = features[72:77]
    _ALL_YEAR_DOMESTIC_MIGRATION = features[67:77]
    df_relative_analysis["2_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_2_YEAR_DOMESTIC_MIGRATION)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DOMESTIC_MIGRATION"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # INTERNATIONAL MIGRATION
    _2_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[65:67]
    _3_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[64:67]
    _5_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[62:67]
    _ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE = features[57:67]
    df_relative_analysis["2_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(_2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(_3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(_5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_INTERNATIONAL_MIGRATION"] = df[df.columns.intersection(_ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # MOST DEATHS 2, 3, 5, ALL YEARS
    _2_YEAR_DEATHS_SLICE = features[45:47]
    _3_YEAR_DEATHS_SLICE = features[44:47]
    _5_YEAR_DEATHS_SLICE = features[42:47]
    _ALL_YEAR_DEATHS_SLICE = features[37:47]
    df_relative_analysis["2_YEAR_DEATHS"] = df[df.columns.intersection(_2_YEAR_DEATHS_SLICE)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DEATHS"] = df[df.columns.intersection(_3_YEAR_DEATHS_SLICE)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DEATHS"] = df[df.columns.intersection(_5_YEAR_DEATHS_SLICE)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DEATHS"] = df[df.columns.intersection(_ALL_YEAR_DEATHS_SLICE)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # MOST BIRTHS 2, 3, 5, ALL YEARS
    _2_YEAR_BIRTHS_SLICE = features[35:37]
    _3_YEAR_BIRTHS_SLICE = features[34:37]
    _5_YEAR_BIRTHS_SLICE = features[32:37]
    _ALL_YEAR_BIRTHS_SLICE = features[27:37]
    df_relative_analysis["2_YEAR_BIRTHS"] = df[df.columns.intersection(_2_YEAR_BIRTHS_SLICE)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_BIRTHS"] = df[df.columns.intersection(_3_YEAR_BIRTHS_SLICE)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_BIRTHS"] = df[df.columns.intersection(_5_YEAR_BIRTHS_SLICE)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_BIRTHS"] = df[df.columns.intersection(_ALL_YEAR_BIRTHS_SLICE)].mean(axis=1) / _ALL_YEAR_POP_AVG

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
    df_abs_analysis["2_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_2_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["3_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)

    df_abs_analysis["FIPS"] = df_abs_analysis.index.map(dict(mappings.cbsa_fips_df.values))
    df_abs_analysis["STATE"] = df_abs_analysis["FIPS"].map(mappings.states_df["state"])
    df_abs_states = df_abs_analysis.groupby("STATE").sum()

    # RANKINGS & REPORTS
    


    # SAVE TO EXCEL
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "reports", "population.xlsx")
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df_relative_analysis.to_excel(writer, sheet_name="relative_CBSA_analysis")
    df_abs_analysis.to_excel(writer, sheet_name="absolute_CBSA_analysis")
    df_abs_states.to_excel(writer, sheet_name="state_aggregation")
    writer.save()


def main():
    cb = CensusBureau()
    analyze_population(cb)


if __name__ == "__main__":
    main()
