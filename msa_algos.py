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
    mappings.cbsa_fips_df = mappings.cbsa_fips_df[["cbsacode", "fipsstatecode"]].drop_duplicates()
    df = df.groupby("CBSA").sum()

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

    # MOST DEATH 2, 3, 5, ALL YEAR
    _2_YEAR_DEATHS = features[45:47]
    _3_YEAR_DEATHS = features[44:47]
    _5_YEAR_DEATHS = features[42:47]
    _ALL_YEAR_DEATHS = features[37:47]
    df_relative_analysis["2_YEAR_DEATHS"] = df[df.columns.intersection(_2_YEAR_DEATHS)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_DEATHS"] = df[df.columns.intersection(_3_YEAR_DEATHS)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_DEATHS"] = df[df.columns.intersection(_5_YEAR_DEATHS)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_DEATHS"] = df[df.columns.intersection(_ALL_YEAR_DEATHS)].mean(axis=1) / _ALL_YEAR_POP_AVG

    # MOST BIRTHS - 3/5 YEARS
    _2_YEAR_BIRTHS = features[35:37]
    _3_YEAR_BIRTHS = features[34:37]
    _5_YEAR_BIRTHS = features[32:37]
    _ALL_YEAR_BIRTHS = features[27:37]
    df_relative_analysis["2_YEAR_BIRTHS"] = df[df.columns.intersection(_2_YEAR_BIRTHS)].mean(axis=1) / _2_YEAR_POP_AVG
    df_relative_analysis["3_YEAR_BIRTHS"] = df[df.columns.intersection(_3_YEAR_BIRTHS)].mean(axis=1) / _3_YEAR_POP_AVG
    df_relative_analysis["5_YEAR_BIRTHS"] = df[df.columns.intersection(_5_YEAR_BIRTHS)].mean(axis=1) / _5_YEAR_POP_AVG
    df_relative_analysis["ALL_YEAR_BIRTHS"] = df[df.columns.intersection(_ALL_YEAR_BIRTHS)].mean(axis=1) / _ALL_YEAR_POP_AVG


    """
    START ABSOLUTE ANALYSIS
    """

    df_abs_analysis = pd.DataFrame(index=df.index)
    #df_abs_analysis["FIPS"] = df["FIPS"]
    #df_abs_analysis["STATE"] = df["STATE"]

    # DEFINE LOOKBACKS
    _3_YEAR_LOOKBACK = features[24:27]
    _5_YEAR_LOOKBACK = features[22:27]
    _ALL_PERIODS_LOOKBACK = features[17:27]

    # AGGREGATE ABSOLUTES: 1, 3, 5 YEARS
    df_abs_analysis["2019_ABS_CHANGE"] = df["NPOPCHG2019"]
    df_abs_analysis["3_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_3_YEAR_LOOKBACK)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_5_YEAR_LOOKBACK)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_CHANGE"] = df[df.columns.intersection(_ALL_PERIODS_LOOKBACK)].sum(axis=1)

    # BIRTHS
    df_abs_analysis["2019_ABS_BIRTHS"] = df["BIRTHS2019"]
    df_abs_analysis["3_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_3_YEAR_BIRTHS)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_5_YEAR_BIRTHS)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_BIRTHS"] = df[df.columns.intersection(_ALL_YEAR_BIRTHS)].sum(axis=1)

    # DEATHS
    df_abs_analysis["2019_ABS_DEATHS"] = df["DEATHS2019"]
    df_abs_analysis["3_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_3_YEAR_DEATHS)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_5_YEAR_DEATHS)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DEATHS"] = df[df.columns.intersection(_ALL_YEAR_DEATHS)].sum(axis=1)

    # DOMESTIC
    df_abs_analysis["2019_ABS_DOMESTIC"] = df["DOMESTICMIG2019"]
    df_abs_analysis["3_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_3_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_5_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_DOMESTIC"] = df[df.columns.intersection(_ALL_YEAR_DOMESTIC_MIGRATION)].sum(axis=1)

    # INTERNATIONAL
    df_abs_analysis["2019_ABS_INTERNATIONAL"] = df["INTERNATIONALMIG2019"]
    df_abs_analysis["3_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_3_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["5_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_5_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)
    df_abs_analysis["ALL_YEAR_ABS_INTERNATIONAL"] = df[df.columns.intersection(_ALL_YEAR_INTERNATIONAL_MIGRATION_SLICE)].sum(axis=1)

    df_abs_analysis["FIPS"] = df_abs_analysis.index.map(dict(mappings.cbsa_fips_df.values))
    df_abs_analysis["STATE"] = df_abs_analysis["FIPS"].map(mappings.states_df["state"])
    df_abs_states = df_abs_analysis.groupby("STATE").sum()

    # SAVE TO EXCEL

    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "reports", "population.xlsx")
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    workbook = writer.book
    # worksheet = workbook.add_worksheet('relative_CBSA_analysis')
    # worksheet2 = workbook.add_worksheet('absolute_CBSA_analysis')
    # worksheet3 = workbook.add_worksheet('state_aggregation')

    # writer["relative_CBSA_analysis"] = worksheet
    # writer["absolute_CBSA_analysis"] = worksheet2
    # writer["state_aggregation"] = worksheet3

    df_relative_analysis.to_excel(writer, sheet_name="relative_CBSA_analysis")
    df_abs_analysis.to_excel(writer, sheet_name="absolute_CBSA_analysis")
    df_abs_states.to_excel(writer, sheet_name="state_aggregation")
    writer.save()


def main():
    cb = CensusBureau()
    analyze_population(cb)


if __name__ == "__main__":
    main()
