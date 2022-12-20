"""
This function contains python-bigquery to interact with BigQuery GCP API. These scripts need to be more efficient and
general as they can.
"""

from utils.local import local_env
from google.cloud import bigquery
import pandas as pd


def get_bigquery_client():
    """
    Get credentials from GOOGLE_APPLICATION_CREDENTIALS system environment variable and returns a BigQuery
    Returns:
        BigQuery client

    """
    credentials = local_env.get_gcp_credentials()
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return bq_client


def get_closest_weather_station(latitude: float, longitude: float, bigquery_client: bigquery.client) -> pd.DataFrame:
    """
    This script returns 3 nearest weather station at latitude and longitude.
    The scripts use the GHCN public weather database.

    Args:
        latitude:[float] the latitude of the position that you want to get weather information
        longitude:[float] the longitude of the position that you want to get weather information
        bigquery_client:
    """
    job_config = bigquery.QueryJobConfig(use_legacy_sql=True)

    bigquery_query = f"""SELECT name, id, state, latitude, longitude,
    DEGREES(ACOS(SIN(RADIANS(latitude)) * SIN(RADIANS({latitude})) + COS(RADIANS(latitude)) * COS(RADIANS({latitude}))
     * COS(RADIANS(longitude - {longitude})))) * 60 * 1.515 * 1.609344 AS dist_kms
     FROM 
        [bigquery-public-data:ghcn_d.ghcnd_stations]
    ORDER BY
        dist_kms ASC
    LIMIT 3 
    """

    stations = bigquery_client.query(bigquery_query, job_config=job_config).to_dataframe()

    return stations


def get_weather_data_by_feature(station_id, year, bigquery_client: bigquery.client):
    """

    Args:
        station_id:
        year:
        bigquery_client:

    Returns:

    """
    sql_fetch = f"""SELECT STRING(wx2.date) AS date, MAX(prcp) AS prcp, MAX(tmin) AS tmin, MAX(tmax) AS tmax, 
                            IF(MAX(haswx) = 'True', 'True', 'False') AS haswx 
                    FROM (SELECT wx.date, IF (wx.element = 'PRCP', wx.value/10, NULL) AS prcp,
                                IF (wx.element = 'TMIN', wx.value/10, NULL) AS tmin, 
                                IF (wx.element = 'TMAX', wx.value/10, NULL) AS tmax, 
                                IF (SUBSTR(wx.element, 0, 2) = 'WT', 'True', NULL) AS haswx
                            FROM `bigquery-public-data.ghcn_d.ghcnd_{year}` AS wx
                    WHERE id = '{station_id}'
                    AND qflag IS NULL ) as wx2
                    GROUP BY date
                    ORDER BY date"""

    data_weather = bigquery_client.query(sql_fetch).to_dataframe()
    data_weather['date'] = pd.to_datetime(data_weather['date'])
    data_weather.set_index("date", inplace=True)

    return data_weather


def t_get_closest_weather_station():
    """
    pass
    """
    latitude_ = 48.92449951
    longitude_ = 2.22220993
    dataframe = get_closest_weather_station(latitude_, longitude_, client)
    print(dataframe)


client = get_bigquery_client()
data = get_weather_data_by_feature("FRM00007156", 2022, client)
print(data.head(3))
