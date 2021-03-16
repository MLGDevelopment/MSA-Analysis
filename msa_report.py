import pandas as pd
from DataScraper import Mappings
from Axio import Axio
from Costar import CostarMultifamily
from CensusBureau import CensusBureau
import numpy as np
PERMIT_LOOKBACK = 36


# IMPORT MAPPINGS
mappings = Mappings()
cb = CensusBureau()
# GET ACS 5 YEAR DATA FOR MEDIAN HHI
cb_report = cb.build_acs_report()
cb_report['CBSA Code'] = cb_report['combined_county_id'].map(mappings.cbsa_fips_df['CBSA Code'])
df = cb.load_population_data()
df['CBSA Code'] = df['COMBINED_COUNTY_ID'].astype(str).map(mappings.cbsa_fips_df['CBSA Code'])
df["Metropolitan Division Code"] = df['COMBINED_COUNTY_ID'].astype(str).map(mappings.cbsa_fips_df['Metropolitan Division Code'])
# IMPORT CENSUS BUREAU PERMITS
cb_monthly_permits = cb.load_permit_dataset()

# IMPORT AXIO DATA
axio = Axio()

# IMPORT COSTAR DATA
costar_mf = CostarMultifamily()

# GET LIST OF CBSA IDS
cbsa_list = [i for i in set(mappings.cbsa_fips_df['CBSA Code']) if i == i]

master = []

for cbsa_id in cbsa_list:

    # GET CBSA/MSA TITLE
    cbsa_name = mappings.cbsa_fips_df[mappings.cbsa_fips_df['CBSA Code'] == cbsa_id]['CBSA Title'][0]
    cbsa_population_data = df[df['CBSA Code'] == cbsa_id]

    # NEED TO RESET INDEX SO ILOC REFS ARE CORRECT
    cbsa_population_data.reset_index(drop=True, inplace=True)

    # need to filter on cbsa code
    cbsa_population_current = pd.pivot_table(cbsa_population_data, values='estimate', index=['year', 'level_7'], aggfunc=np.sum)
    cbsa_population_current.reset_index(drop=False, inplace=True)

    if not cbsa_population_current.empty:
        curr_population_estimate = cbsa_population_current[cbsa_population_current['level_7'] == 'POPESTIMATE'].sort_values('year')['estimate'].iloc[-1]
        population_5_years_ago = cbsa_population_current[cbsa_population_current['level_7'] == 'POPESTIMATE'].sort_values('year')['estimate'].iloc[-5]
        population_5_year_growth = curr_population_estimate / population_5_years_ago - 1
        total_change_5_years = cbsa_population_current[cbsa_population_current['level_7'] == 'NPOPCHG_'].sort_values('year')['estimate'].iloc[-5:].sum()
        net_migration_past_5_years = cbsa_population_current[cbsa_population_current['level_7'] == 'NETMIG'].sort_values('year')['estimate'].iloc[-5:].sum()
        net_migration_pct_of_total_change = net_migration_past_5_years / total_change_5_years

    else:
        net_migration_past_5_years = '-'
        net_migration_pct_of_total_change = '-'
        curr_population_estimate = '-'
        population_5_year_growth = '-'

    # need to check for Metro ID
    if not pd.isnull(df[df['CBSA Code'] == cbsa_id]["Metropolitan Division Code"].iloc[0]):
        temp = list(set(df[df['CBSA Code'] == cbsa_id]["Metropolitan Division Code"].values.tolist()))
        cbsa_package = [cbsa_id] + temp
    else:
        cbsa_package = cbsa_id

    costar_rent_growth_12_month = costar_mf.get_12_month_effective_rent_growth(cbsa_package)
    costar_rent = costar_mf.get_most_recent_cbsa_rent(cbsa_package)
    costar_occupancy = costar_mf.get_most_recent_occupancy(cbsa_package)
    axio_rent_growth_12_month = axio.get_12_month_rent_cbsa_growth(cbsa_package)
    axio_rent = axio.get_current_cbsa_rent(cbsa_package)
    axio_occupancy = axio.get_current_cbsa_occupancy(cbsa_package)

    cb_acs5_data = cb_report[(cb_report['CBSA Code'] == cbsa_id)]
    cbsa_acs5_data_2019 = cb_acs5_data[cb_acs5_data['year'] == 2019]
    cb_acs5_pivot = pd.pivot_table(cbsa_acs5_data_2019,
                                   values=['Total Households', 'Average Household Size', 'Median Household Income',
                                           'Median Age', 'Occupied Housing Units', 'High school graduate, GED, or alternative',
                                           "Bachelor\'s degree or higher"], index=['CBSA Code', 'year'], aggfunc=np.mean)


    cb_acs5_pivot_all_years = pd.pivot_table(cb_report[(cb_report['CBSA Code'] == cbsa_id)],  values=['Total Households', 'Average Household Size', 'Median Household Income','Median Age', 'Occupied Housing Units', 'High school graduate, GED, or alternative', "Bachelor\'s degree or higher"], index=['CBSA Code', 'year'], aggfunc=np.mean)
    cb_acs5_pivot_all_years = cb_acs5_pivot_all_years.reset_index(drop=False)

    cb_acs5_pivot_all_years.sort_values('year', inplace=True)
    median_hhi_5_yr_growth = cb_acs5_pivot_all_years.iloc[-1, :]['Median Household Income'] / cb_acs5_pivot_all_years.iloc[-5, :]['Median Household Income'] - 1

    median_hhi = cb_acs5_pivot['Median Household Income'].values[0]
    total_households = cb_acs5_pivot['Total Households'].values[0]
    average_household_size = cb_acs5_pivot['Average Household Size'].values[0]
    # median_age = cb_acs5_pivot['Median Age'].values[0]
    # hs_rate = cb_acs5_pivot['High school graduate, GED, or alternative'].values[0]
    # college_rate = cb_acs5_pivot["Bachelor\'s degree or higher"].values[0]

    rental_affordability = ((costar_rent + axio_rent) / 2) / (median_hhi / 12)

    # need to error check
    cbsa_permits_last_3_years_average, cbsa_permits_historical_average = cb.get_permit_trend(cbsa_id, 36)

    record = {"cbsa": cbsa_id,
              "MSA": cbsa_name,
              "2019 Population Est.": curr_population_estimate,
              "5 Yr Population % Change": population_5_year_growth,
              "MSA (All counties) 5 Yr Net Migration": net_migration_past_5_years,
              "MSA (All counties) Net Migration as a % of total growth": net_migration_pct_of_total_change,
              "Median HHI": median_hhi,
              "5 Yr Growth Median HHI": median_hhi_5_yr_growth,
              "Rent - Costar": costar_rent,
              "Rent - Axio": axio_rent,
              "Costar 12 Month Growth": costar_rent_growth_12_month,
              "Axio 12 Month Growth": axio_rent_growth_12_month,
              "Occupancy - Costar": costar_occupancy,
              "Occupancy - Axio": axio_occupancy,
              "Housing Affordability": rental_affordability,
              "5+ Unit Permits - 3 Yr Avg": cbsa_permits_last_3_years_average,
              "5+ Unit Permits - Hist. Avg. before last 3 Yrs": cbsa_permits_historical_average}

    master.append(record)

master_df = pd.DataFrame(master)
master_df.to_excel('cbsa-report.xlsx')
