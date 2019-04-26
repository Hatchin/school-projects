import plotly.plotly as py
import pandas as pd
import matplotlib
from matplotlib import cm
import numpy as np

import plotly.graph_objs as go

happiness = '2015.csv'
location = 'average-latitude-longitude-countries.csv'
country_code = 'all.csv'

happiness_df = pd.read_csv(happiness)
location_df = pd.read_csv(location)
leng_df = pd.read_csv(country_code)

location_df['Country'][38] = 'Congo (Kinshasa)'
location_df['Country'][40] = 'Congo (Brazzaville)'
location_df['Country'][104] = 'Iran'
location_df['Country'][42] = 'Ivory Coast'
location_df['Country'][121] = 'Laos'
location_df['Country'][131] = 'Libya'
location_df['Country'][134] = 'Moldova'
location_df['Country'][52] = 'North Cyprus'
location_df['Country'][176] = 'Palestinian Territories'
location_df['Country'][184] = 'Russia'
location_df['Country'][199] = 'Somaliland region'
location_df['Country'][116] = 'South Korea'
location_df['Country'][203] = 'Syria'
location_df['Country'][219] = 'Tanzania'

country_df = pd.merge(happiness_df, location_df, on = 'Country', )
country_df = pd.merge(country_df, leng_df[['alpha-2', 'alpha-3']], left_on='ISO 3166 Country Code', right_on='alpha-2')

# 1. Choropleth Maps

data = [ dict(
        type = 'choropleth',
        locations = country_df['alpha-3'],
        z = country_df['Happiness Score'],
        text = country_df['Country'],
        colorscale = 'Portland',
        # colorscale = [[0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
        #    [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"]],
        autocolorscale = False,
        reversescale = False,
        marker = dict(
            line = dict (
                color = 'rgb(180,180,180)',
                width = 0.5
            ) ),
        colorbar = dict(
            autotick = False,
            title = 'Happiness Score'),
      ) ]

layout = dict(
    title = '2015 Global Happiness Score<br>Source:\
            <a href="https://www.kaggle.com/unsdsn/world-happiness/data">Kaggle </a>',
    geo = dict(
        geo = dict(
        showframe = False,
        showcoastlines = False,
        projection = dict(
            type = 'Mercator'
        )
    )
    )
)

fig_1 = dict( data=data, layout=layout )
py.iplot( fig_1, validate=False, filename='d3-happiness' )

# 2. Symbol Maps

country_df['text'] = country_df['Country'] + '<br>Rank : ' +\
                    country_df['Happiness Rank'].astype(str) + \
                    '<br>Score: ' + country_df['Happiness Score'].astype(str)
data_2 = [go.Scattergeo(
            lon = country_df['Longitude'],
            lat = country_df['Latitude'],
            text = country_df['text'],
            marker = dict(
                size = country_df['Happiness Score']*1.12,
                color = country_df['Happiness Score'], #set color equal to a variable
                colorscale='Portland',
                showscale=True,
                line = dict(width = 0))
            )
         ]

layout = go.Layout(
    title = 'Country Happiness Score 2015<br> \
Source: <a href="https://www.kaggle.com/unsdsn/world-happiness/data">\
Kaggle</a>',
    geo = dict(
        resolution = 50,
        scope = 'world',
        showcountries = True,
        showcoastlines = True,
        showland = True,
        countrywidth = .5,
        landcolor = "rgb(229, 229, 229)",
        countrycolor = "rgb(255, 255, 255)" ,
        coastlinecolor = "rgb(255, 255, 255)",
        projection = dict(
            type = 'Mercator'
        ),
        domain = dict(
            x = [ 0, 1 ],
            y = [ 0, 1 ]
        )
    )
)

fig = go.Figure(layout=layout, data=data_2)
py.iplot(fig, validate=False, filename='Happiness Score')