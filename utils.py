import copy
from shapely.wkt import loads
import geopandas as gpd

from data_models.common_db import engine
from data_models.osm_model import *


def create_table(table_obj):

    try:
        table_obj.__table__.drop(bind=engine, checkfirst=True)
        table_obj.__table__.create(bind=engine)
        return

    except Exception as e:
        print(e)
        exit(-1)


LOS_ANGELES_OSM = {
    'landuse_a': CaliforniaOsmLanduseA,
    'natural': CaliforniaOsmNatural,
    'natural_a': CaliforniaOsmNaturalA,
    'places': CaliforniaOsmPlaces,
    'places_a': CaliforniaOsmPlacesA,
    'pois': CaliforniaOsmPois,
    'pois_a': CaliforniaOsmPoisA,
    'pofw': CaliforniaOsmPofw,
    'pofw_a': CaliforniaOsmPofwA,
    'railways': CaliforniaOsmRailways,
    'roads': CaliforniaOsmRoads,
    'traffic': CaliforniaOsmTraffic,
    'traffic_a': CaliforniaOsmTrafficA,
    'transport': CaliforniaOsmTransport,
    'transport_a': CaliforniaOsmTransportA,
    'water_a': CaliforniaOsmWaterA,
    'waterways': CaliforniaOsmWaterway
}

LOS_ANGELES_BOUNDING_BOX = 'POLYGON((-118.5246 33.7322, -118.5246 34.1455, -118.1158 34.1455, -118.1158 33.7322, ' \
                           '-118.5246 33.7322))'

LOS_ANGELES_BOUNDING_BOX2 = 'POLYGON((-119.059 33.697, -119.059 34.816, -117.545 34.816, -117.545 33.697, ' \
                           '-119.059 33.697))'
