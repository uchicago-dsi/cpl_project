#Access data from Mapbox Isochrone API
#Author: Stephania Tello Zamudio

import requests
from urllib.request import urlopen
import json
from DO_NOT_COMMIT_rajvi_mapbox_token import my_token

TOKEN = my_token()

class MapboxAPI:
    '''
    This class extracts areas from Mapbox Isochrone API that are reachable within 
    a specified amount of time from a location
    
    '''
    def __init__(self):
        '''
        This function initializes a new instance of the MapboxAPI class
        
        Inputs:
            - token (str): a string representing the API token needed to access
            Mapbox Isochrone API. It can be obtained by creating an account and
            generating a new token (free up to 100,000 requests).
        '''
        
        self.token = TOKEN
        self.base_url = 'https://api.mapbox.com/isochrone/v1/mapbox/walking/' 
        #note that driving is an option (other is walking), look at isochrone documentation
        
    def get_data(self, coordinates, contours_minutes):
        '''
        This method extracts data from the Mapbox Isochrone API for the
        specified coordinates and contours_minutes.
        
        Input:
        - coordinates (tuple): A (longitude,latitude) coordinate pair around which 
        to center the isochrone lines.
        - contours_minutes (int): The times, in minutes, to use for each isochrone 
        contour.
        
        Returns:
        - polygon coordinates (list): A list containing the reachable regions as 
        contours of polygons or lines within the specified minutes
        '''
        #15-minute driving distance (indicated in project brief)
        
        latitude, longitude = coordinates
        
        full_url = f'{self.base_url}{longitude},{latitude}?contours_minutes={str(contours_minutes)}&polygons=true&access_token={self.token}'
        #Note that polygons=true returns the contours as GeoJSON polygons (if false they would be linestrings)
        data_response = requests.get(full_url)
        
        full_json = data_response.json()
        
        #geometry = full_json['features'][0]['geometry']['coordinates'][0] #coordinates of polygon
        geometry = full_json['features'][0]['geometry']

        return geometry
