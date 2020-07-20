from geoalchemy2 import WKTElement
from sqlalchemy import func

from data_models.common_db import engine, session
from data_models.grid_model import *
from utils import create_table, LOS_ANGELES_BOUNDING_BOX


def generate_grids(config, area=None):

    # bounding_box = WKTElement(config['BOUNDING_BOX'], srid=4326)  # This one is not working
    grid_obj = config['GRID_OBJ']
    resolution = config['RESOLUTION']
    epsg = config['EPSG']

    try:

        grids = session.query(func.ST_Dump(
            func.makegrid_2d(func.ST_GeomFromText(config['BOUNDING_BOX'], 4326), resolution, resolution)).geom.label('geom')  # self-defined function in Psql
        ).subquery()

        # using the boundary to crop the area
        # if config['AREA'] == 'los_angeles':
        #     grids = session.query(grids.c.geom) \
        #         .filter(func.ST_Intersects(LosAngelesCountyBoundary.wkb_geometry, grids.c.geom)).subquery()

        results = session.query(
            func.row_number().over().label('gid'),
            func.ST_Centroid(grids.c.geom).label('centroid'),
            func.ST_X(func.ST_Centroid(grids.c.geom)).label('lon'),
            func.ST_Y(func.ST_Centroid(grids.c.geom)).label('lat'),
            grids.c.geom,
            func.ST_X(func.ST_Transform(func.ST_Centroid(grids.c.geom), epsg)).label('lon_proj'),
            func.ST_Y(func.ST_Transform(func.ST_Centroid(grids.c.geom), epsg)).label('lat_proj')).all()

        obj_results = []
        for res in results:
            obj_results.append(grid_obj(gid=res[0], centroid=res[1], lon=res[2], lat=res[3],
                                        geom=res[4], lon_proj=res[5], lat_proj=res[6]))
        # session.add_all(obj_results)
        # session.commit()
        return

    except Exception as e:
        print(e)
        exit(-1)


def main(area, res):

    LOS_ANGELES = {
        'BOUNDING_BOX': LOS_ANGELES_BOUNDING_BOX,
        'EPSG': 6423,
        'GRID_OBJ_500': LosAngeles500mGrid,
        'GRID_OBJ_1000': LosAngeles1000mGrid,
        'GRID_OBJ_5000': LosAngeles5000mGrid,
    }

    conf = dict()

    if area == 'los_angeles':
        conf['AREA'] = area
        conf['BOUNDING_BOX'] = LOS_ANGELES['BOUNDING_BOX']
        conf['EPSG'] = LOS_ANGELES['EPSG']
        conf['RESOLUTION'] = res
        conf['GRID_OBJ'] = LOS_ANGELES[f'GRID_OBJ_{res}']
    else:
        pass

    """ !!! Be careful, create table would overwrite the original table """
    # create_table(conf['GRID_OBJ'])
    generate_grids(conf)
