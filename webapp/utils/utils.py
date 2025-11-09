import numpy as np
from datetime import datetime,timedelta, date
# check your pickle compability, perhaps its pickle not pickle5
import pandas as pd
import json


#loading data
def load_data():

    df_tot_confirmed = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv', encoding='utf-8', na_values=None)
    df_tot_death = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv', encoding='utf-8', na_values=None)

    df_tot_recovered = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv', encoding='utf-8', na_values=None)
    df_tot_confirmed.replace(
        to_replace='US', value='United States', regex=True, inplace=True)
    df_tot_recovered.replace(
        to_replace='US', value='United States', regex=True, inplace=True)
    df_tot_death.replace(
        to_replace='US', value='United States', regex=True, inplace=True)
    # I need data that contain population for each country to calculate confirmed cases/population
    # I download it from : https://github.com/samayo/country-json/blob/master/src/country-by-population.json
    df_pop = pd.read_json(
        'https://raw.githubusercontent.com/samayo/country-json/master/src/country-by-population.json')
    #some country name has different  format, so I need to change it to match my first dataset
    df_pop.columns = ['Country/Region', 'population']
    df_pop = df_pop.replace(to_replace='Russian Federation', value='Russia')
    

    return df_tot_confirmed, df_tot_death, df_tot_recovered, df_pop


def preprocessed_data(df_tot_confirmed, df_tot_death, df_tot_recovered):
    #grouped total confirmed data
    df_gt_confirmed = df_tot_confirmed[["Country/Region", df_tot_confirmed.columns[-1]]].groupby(
        "Country/Region").sum().sort_values(by=df_tot_confirmed.columns[-1], ascending=False)
    df_gt_confirmed.reset_index(inplace=True)
    df_gt_confirmed.columns = ["Country/Region", 'confirmed']
    df_gt_confirmed.replace(
        to_replace='US', value='United States', regex=True, inplace=True)

    #Chart.js can't plot dataframe object, so we need to change some to list
    barplot_confirmed_values = df_gt_confirmed["confirmed"].values.tolist(
    )
    country_names = df_gt_confirmed["Country/Region"].values.tolist()

    #global time series confirmed data frame
    df_global_confirmed = pd.DataFrame(
        df_tot_confirmed[df_tot_confirmed.columns[4:]].sum())
    df_global_confirmed.reset_index(inplace=True)
    df_global_confirmed.columns = ['date', 'total confirmed']

    #global daily new cases = global daily confirmed at date (t) -  global daily confirmed at date (t-1)
    df_global_confirmed["daily new cases"] = df_global_confirmed['total confirmed'] - \
        df_global_confirmed['total confirmed'].shift()
    df_global_confirmed = df_global_confirmed.fillna(0)

    #grouped total recovered data
    df_gt_recovered = df_tot_recovered[["Country/Region", df_tot_recovered.columns[-1]]].groupby(
        "Country/Region").sum().sort_values(by=df_tot_recovered.columns[-1], ascending=False)
    df_gt_recovered.reset_index(inplace=True)
    df_gt_recovered.columns = ["Country/Region", 'recovered']
    df_gt_recovered.replace(
        to_replace='US', value='United States', regex=True, inplace=True)

    #Chart.js can't plot dataframe object, so we need to change some to list
    barplot_recovered_values = df_gt_recovered["recovered"].values.tolist(
    )
    country_names = df_gt_recovered["Country/Region"].values.tolist()

    #global time series recovered data frame
    df_global_recovered = pd.DataFrame(
        df_tot_recovered[df_tot_recovered.columns[4:]].sum())
    df_global_recovered.reset_index(inplace=True)
    df_global_recovered.columns = ['date', 'total recovered']

    #global daily recovered = global daily recovered at date (t) -  global daily recovered at date (t-1)
    df_global_recovered["daily new recovered"] = df_global_recovered['total recovered'] - \
        df_global_recovered['total recovered'].shift()
    df_global_recovered = df_global_recovered.fillna(0)

    # grouping the data by each country to get total confirmed cases
    df_gt_death = df_tot_death[["Country/Region", df_tot_death.columns[-1]]].groupby(
        "Country/Region").sum().sort_values(by=df_tot_death.columns[-1], ascending=False)
    df_gt_death.reset_index(inplace=True)
    df_gt_death.columns = ["Country/Region", 'deaths']
    df_gt_death.replace(
        to_replace='US', value='United States', regex=True, inplace=True)

    #Chart.js can't plot dataframe object, so we need to change some to list
    barplot_death_values = df_gt_death["deaths"].values.tolist()
    df_global_death = df_tot_death[df_tot_death.columns[4:]].sum()

    #global time series death data frame
    df_global_death = pd.DataFrame(
        df_tot_death[df_tot_death.columns[4:]].sum())
    df_global_death.reset_index(inplace=True)
    df_global_death.columns = ['date', 'total deaths']

    #global daily deaths = global daily deaths at date (t) -  global daily deaths at date (t-1)
    df_global_death["daily new deaths"] = df_global_death['total deaths'] - \
        df_global_death['total deaths'].shift()
    df_global_death = df_global_death.fillna(0)
    df_global_death

    #merge all the data to get full time series dataframe
    df_timeseries_final = pd.merge(
        df_global_confirmed, df_global_recovered, how='inner', on='date')
    df_timeseries_final = pd.merge(
        df_timeseries_final, df_global_death, how='inner', on='date')
    df_timeseries_final
    return df_gt_confirmed, df_gt_recovered, df_gt_death, df_timeseries_final, country_names


def merge_data(df_gt_confirmed, df_gt_recovered, df_gt_death, df_pop):
  # I also need country code for geographical analysis, Altair need numerical code and Plotly need alfabetical code
    #country code and id for later geographical analysis
    url = "https://gist.githubusercontent.com/komasaru/9303029/raw/9ea6e5900715afec6ce4ff79a0c4102b09180ddd/iso_3166_1.csv"
    country_code = pd.read_csv(url)
    country_code = country_code[[
        "English short name", "Alpha-3 code", "Numeric"]]
    country_code.columns = ["Country/Region", "code3", "id"]

    #Change the data for later merging
    #If not match the value will be deleted, so we need to make sure each country name from each table has same value
    country_code = country_code.replace(
        to_replace='Russian Federation (the)', value='Russia')
    country_code = country_code.replace(
        to_replace='United Kingdom (the)', value='United Kingdom')
    country_code = country_code.replace(
        to_replace='United States (the)', value='United States')
    country_code = country_code.replace(to_replace='Viet Nam', value='Vietnam')

    # merge them all
    final_df = pd.merge(df_gt_confirmed,
                        df_gt_recovered, how='inner', on='Country/Region')
    final_df = pd.merge(final_df, df_gt_death,
                        how='inner', on='Country/Region')
    final_df = pd.merge(final_df, df_pop, how='inner', on='Country/Region')
    final_df = pd.merge(country_code, final_df,
                        how='inner', on='Country/Region')
    final_df = final_df.sort_values(by="confirmed", ascending=False)
    final_df.reset_index(inplace=True, drop=True)

    # calculate cases/million and total death rate
    final_df['cases/million'] = ((final_df['confirmed'] /
                                  final_df['population'])*1000000).round(2)
    final_df['death rate(%)'] = (
        (final_df['deaths']/final_df['confirmed'])*100).round(2)

    return final_df

# function to filter timeseries analysis by country
# I use "case" variable just for column name: e.g, case = confirmed, case = deaths


def get_by_country(df, country, case):
    mask = (df['Country/Region'] == country)
    df = df.loc[mask]
    df_country = df.groupby("Country/Region").sum()
    df_country = pd.DataFrame(df[df.columns[4:]].sum())
    df_country.reset_index(inplace=True)
    df_country.columns = ['date', f"value_{case}"]
    df_country[f"daily_new_{case}"] = df_country[f"value_{case}"] - \
        df_country[f"value_{case}"].shift()
    df_country = df_country.fillna(0)
    return df_country


#use function above to get merged dataframe
def get_by_country_merged(df_tot_confirmed, df_tot_death, df_tot_recovered, country):
    #apply to each timeseries
    df_country_confirmed_tseries = get_by_country(
        df_tot_confirmed, country, "confirmed")
    df_country_death_tseries = get_by_country(df_tot_death, country, "death")
    df_country_recovered_tseries = get_by_country(
        df_tot_recovered, country, "recovered")

    #merge them all
    df_country_timeseries_final = pd.merge(
        df_country_confirmed_tseries, df_country_death_tseries, how='inner', on='date')
    df_country_timeseries_final = pd.merge(
        df_country_timeseries_final, df_country_recovered_tseries, how='inner', on='date')
    df_country_timeseries_final.reset_index(inplace=True)
    return df_country_timeseries_final


def get_per_country_data(df_tot_confirmed, df_tot_death, df_tot_recovered, country_name):
    #total confirmed per country
    total_confirmed_per_country = df_tot_confirmed.groupby(
        "Country/Region").sum()
    total_confirmed_per_country.reset_index(inplace=True)
    mask = (total_confirmed_per_country['Country/Region'] == country_name)
    total_confirmed_per_country = total_confirmed_per_country.loc[mask]
    total_confirmed_per_country = total_confirmed_per_country[
        total_confirmed_per_country.columns[-1]].sum()

    #total deaths per country
    total_death_per_country = df_tot_death.groupby("Country/Region").sum()
    total_death_per_country.reset_index(inplace=True)
    mask = (total_death_per_country['Country/Region'] == country_name)
    total_death_per_country = total_death_per_country.loc[mask]
    total_death_per_country = total_death_per_country[total_death_per_country.columns[-1]].sum(
    )

    #total recovered per country
    total_recovered_per_country = df_tot_recovered.groupby(
        "Country/Region").sum()
    total_recovered_per_country.reset_index(inplace=True)
    mask = (total_recovered_per_country['Country/Region'] == country_name)
    total_recovered_per_country = total_recovered_per_country.loc[mask]
    total_recovered_per_country = total_recovered_per_country[
        total_recovered_per_country.columns[-1]].sum()
    return total_confirmed_per_country, total_death_per_country, total_recovered_per_country
