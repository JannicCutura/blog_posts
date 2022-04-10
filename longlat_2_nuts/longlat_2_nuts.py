
import pandas as pd, numpy as np, seaborn as sns, geopandas as gpd


import matplotlib.pyplot as plt
from shapely.geometry import Point


# import NUTS 3 shape file
nuts3 = gpd.GeoDataFrame.from_file('NUTS_RG_01M_2021_4326.shp')

# import CO2 dataframe
co2 = pd.read_csv("data/v6.0_CO2_excl_short-cycle_org_C_2018_TOTALS.txt", sep=';', skiprows=2)
co2.head()
# define points
co2['coords'] = list(zip(co2['lon'], co2['lat']))
co2['coords'] = co2['coords'].apply(Point)

points = gpd.GeoDataFrame(co2, geometry='coords', crs=nuts3.crs)

# Perform spatial join to match points and polygons
merge = gpd.tools.sjoin(points, nuts3, op="within", how='left')

merge = merge.filter(['lat', 'lon', 'emission 2018 (tons)', 'NUTS_ID', 'LEVL_CODE', 'CNTR_CODE'])
merge.head()

matched = merge[~merge['NUTS_NAME'].isna()]

matched = matched.filter(['lat', 'lon', 'emission 2018 (tons)', 'NUTS_ID', 'LEVL_CODE', 'CNTR_CODE'])

matched.to_parquet("data/eu_emissions.parquet")


matched["emission 2018 (tons)"] = matched["emission 2018 (tons)"]/1000
matched = matched[matched['LEVL_CODE']==3]
agg = matched.groupby("NUTS_ID")["emission 2018 (tons)"].mean().reset_index()
agg["emission 2018 (tons)"] = agg["emission 2018 (tons)"].div(1).round(decimals=0).multiply(1).astype(int)


nuts = nuts3.merge(agg, on="NUTS_ID")
nuts = nuts.filter(["NUTS_ID",'geometry', 'emission 2018 (tons)'])



def plot_map():
    # create figure and axes for Matplotlib
    fig, ax = plt.subplots(1, figsize=(5, 5))

    nuts.plot(column='emission 2018 (tons)',  legend=True,
              cmap='OrRd',   scheme='quantiles',
              linewidth=0.1,
              legend_kwds={'loc':'lower center',
                           'interval': True,
                           'ncol':2,
                           'title':'CO2 emissions (kilo tons)'},
              ax=ax,  edgecolor='0.8')

    # remove the axis
    ax.set_axis_off()

    # add a title
    ax.set_title("European CO2 emissions by NUTS3", fontsize='x-large',fontweight="bold")

    minx, miny, maxx, maxy = -20, 23, 40, 70
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    ax.margins(0)

    plt.tight_layout()
    # saving our map as .png file.
    fig.savefig('map_export.png', dpi=300)


plot_map()









def plot_empty_map():
    # create figure and axes for Matplotlib
    fig, ax = plt.subplots(1, figsize=(5, 5))

    nuts.plot(linewidth=0.1,
              ax=ax,
              edgecolor='0.8')

    # remove the axis
    ax.set_axis_off()
    minx, miny, maxx, maxy = -20, 25, 40, 70
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    plt.tight_layout()
    # saving our map as .png file.
    fig.savefig('empty_map.png', dpi=300)


plot_empty_map()




