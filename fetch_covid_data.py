import time
import pandas as pd
import schedule
timestr = time.strftime("%Y%m%d-%H%M%S")

confirmed_cases_url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
recovered_cases_url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
death_cases_url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

def get_n_melt(data_url,case_type):
    df= pd.read_csv(data_url)
    melted_df = df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'])
    melted_df.rename(columns={"variable": "Date","value":case_type},inplace=True)
    return melted_df

def merge_data(confirmed_df,recovered_df,deaths_df):
	new_df = confirmed_df.join(recovered_df["Recovered"]).join(deaths_df["Deaths"])
	return new_df

def fetch_data():
	"""Fetch and prep data"""
	confirm_df = get_n_melt(confirmed_cases_url,"Confirmed")
	recovered_df = get_n_melt(recovered_cases_url,"Recovered")
	deaths_df = get_n_melt(death_cases_url,"Deaths")

	final_df = merge_data(confirm_df,recovered_df,deaths_df)
	print("Preview Data")
	print(final_df.tail(5))
	filename = "covid19_merged_dataset_updated_{}.csv".format(timestr)
	print("Saving Dataset as {}".format(filename))
	final_df.to_csv(filename)
	print("finished")



#task

schedule.every(5).seconds.do(fetch_data)

while True:
	schedule.run_pending()
	time.sleep(1)