import logging
import pandas as pd
from geopy.geocoders import Nominatim
from airflow.models import Variable

def get_wikipedia_page(url):
    import requests
    logging.info(f'Getting Wikipedia page: {url}')
    try: 
        #get the url
        response = requests.get(url, timeout=10)
        #check if the request is successful
        response.raise_for_status()
        return response.text
    except requests.ResquestException as e:
        logging.error(f'An error occurred: {e}')
        raise 


def get_lat_long(country, city,stadium):
 geolocator= Nominatim(user_agent='mygeoapi')
 location= geolocator.geocode(f'{country},{city},{stadium}')
 if location: 
     return location.latitude, location.longitude
 return 'Location not found'

def preprocess_wikipedia_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull raw data from XCom
    raw_data = ti.xcom_pull(task_ids='extract_data_from_wikipedia', key='raw_data')
    
    if raw_data is None:
        raise ValueError("No raw data found in XCom") 
    df = pd.DataFrame(raw_data)
    df.drop(columns=['Images'],inplace=True)
    df['Stadium'] = df['Stadium'].astype(str).str.replace('♦', '', regex=False).str.strip()
    df['Seating capacity'] = df['Seating capacity'].str.split('[', n=1).str[0].str.strip()
    df['Home team(s)'] = df['Home team(s)'].fillna('No team')
    df['Seating capacity'] = df['Seating capacity'].str.replace(',', '', regex=False).astype(int)
    df['location'] = df.apply(lambda x: get_lat_long(x['Country'], x['City'],x['Stadium']), axis=1)
    ti.xcom_push(key='processed_data', value=df.to_dict(orient='records'))
    return 'Transformation Completed'

def extract_wikipedia_data(**kwargs):
    url= kwargs['url']
    html= get_wikipedia_page(url)
    tables=pd.read_html(url)
    data_df=tables[2]
    kwargs['ti'].xcom_push(key='raw_data', value=data_df.to_dict(orient='records'))
    data_df.to_csv('data/raw_data.csv',index=False)
    return data_df

def write_wikepedia_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull processed data from XCom
    processed_data = ti.xcom_pull(task_ids='preprocess_wikipedia_data', key='processed_data')
    
    if processed_data is None:
        raise ValueError("No processed data found in XCom")
    
    df = pd.DataFrame(processed_data)
    #df.to_csv('data/processed_data.csv', index=False)
    file_name='processed_data.csv'
    df.to_csv('abfs://wikipediaprojectcontainer@wikipediaprojectstorage.dfs.core.windows.net/data/'+file_name,
              storage_options={
                  'account_key':'dpley4qLHOPranFZwusEwq7bt0WXbccrFLjcgW4WBXeXR7sqsBGzfSz/t20j5opC6OhOD15VtJ3B+ASt+0RQ2Q=='
              },index=False)
