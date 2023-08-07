import pandas as pd
import os
import requests


def acquire_people_data():
    filepath='data/people.csv'
    
    # cache if file doesn't exist
    if not os.path.isfile(filepath):
        # get response
        response = requests.get('https://swapi.dev/api/people/')
        # initialize a list to store people data
        people_data = response.json()['results']

        while response.json()['next']:
            # go to next page
            response = requests.get(response.json()['next'])
            # add results to list
            people_data += response.json()['results']

        # store in a DataFrame
        people = pd.DataFrame(people_data)
        
        # save data
        people.to_csv(filepath, index=False)
        
    return pd.read_csv(filepath)

def acquire_planet_data():
    filepath='data/planets.csv'
    
    # cache if file doesn't exist
    if not os.path.isfile(filepath):
        # get response
        response = requests.get('https://swapi.dev/api/planets/')
        # initialize a list to store planet data
        planet_data = response.json()['results']

        while response.json()['next']:
            # go to next page
            response = requests.get(response.json()['next'])

            # add results to list
            planet_data += response.json()['results']

        # store in a DataFrame
        planets = pd.DataFrame(planet_data)
        
        # save data
        planets.to_csv(filepath, index=False)
        
    return pd.read_csv(filepath)

def acquire_starship_data():
    filepath='data/starships.csv'
    
    # cache if file doesn't exist
    if not os.path.isfile(filepath):
        # get response
        response = requests.get('https://swapi.dev/api/starships/')
        # initialize a list to store starship data
        starship_data = response.json()['results']

        while response.json()['next']:
            # go to next page
            response = requests.get(response.json()['next'])
            # add results to list
            starship_data += response.json()['results']

        # store in a DataFrame
        starships = pd.DataFrame(starship_data)
        
        # save data
        starships.to_csv(filepath, index=False)
        

    return pd.read_csv(filepath)

def acquire_ops_data():
    filepath='data/ops_data.csv'
    
    # cache if file doesn't exist
    if not os.path.isfile(filepath):
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        ops = pd.read_csv(url)
        ops.to_csv(filepath,index=False)
        
    return pd.read_csv(filepath)