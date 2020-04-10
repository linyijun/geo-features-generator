from geoalchemy2 import WKTElement
from sqlalchemy import func, literal, Sequence

from data_models.common_db import session, engine
from data_models.grid_model import *
from data_models.geo_feature_grid_model import *
from utils import create_table, LOS_ANGELES_OSM, LOS_ANGELES_BOUNDING_BOX


def crop_osm(osm_table, bounding_box):

    if bounding_box is not None:
        return session.query(osm_table.wkb_geometry, osm_table.fclass) \
            .filter(func.ST_Intersects(osm_table.wkb_geometry, bounding_box)) \
            .filter(osm_table.fclass is not None).subquery()
    else:
        return session.query(osm_table.wkb_geometry, osm_table.fclass) \
            .filter(osm_table.fclass is not None).subquery()


def compute_features_from_osm(config):

    osm_tables = config['OSM']
    bounding_box = WKTElement(config['BOUNDING_BOX'], srid=4326)
    grid_obj = config['GRID_OBJ']
    geo_feature_obj = config['GEO_FEATURE_OBJ']

    try:
        for feature_name, osm_table in osm_tables.items():
            geo_feature_type = osm_table.wkb_geometry.type.geometry_type
            cropped_osm = crop_osm(osm_table, bounding_box)  # crop the OSM data with a bounding box

            sub_query = session.query(grid_obj.gid, cropped_osm.c.fclass,
                                      func.ST_GeogFromWKB(
                                          func.ST_Intersection(grid_obj.geom, cropped_osm.c.wkb_geometry))
                                      .label('intersection')) \
                .filter(func.ST_Intersects(grid_obj.geom, cropped_osm.c.wkb_geometry)).subquery()

            results = []
            if geo_feature_type == 'MULTIPOLYGON':
                results = session.query(sub_query.c.gid.label('gid'),
                                        sub_query.c.fclass.label('feature_type'),
                                        literal(feature_name).label('geo_feature'),
                                        func.SUM(func.ST_AREA(sub_query.c.intersection)).label('value'),
                                        literal('area').label('measurement')) \
                    .group_by(sub_query.c.gid, sub_query.c.fclass).all()

            elif geo_feature_type == 'MULTILINESTRING':
                results = session.query(sub_query.c.gid.label('gid'),
                                        sub_query.c.fclass.label('feature_type'),
                                        literal(feature_name).label('geo_feature'),
                                        func.SUM(func.ST_LENGTH(sub_query.c.intersection)).label('value'),
                                        literal('length').label('measurement')) \
                    .group_by(sub_query.c.gid, sub_query.c.fclass).all()

            elif geo_feature_type == 'POINT':
                results = session.query(sub_query.c.gid.label('gid'),
                                        sub_query.c.fclass.label('feature_type'),
                                        literal(feature_name).label('geo_feature'),
                                        func.COUNT(sub_query.c.intersection).label('value'),
                                        literal('count').label('measurement')) \
                    .group_by(sub_query.c.gid, sub_query.c.fclass).all()

            else:
                pass

            obj_results = []
            for res in results:
                obj_results.append(geo_feature_obj(gid=res[0], feature_type=res[1], geo_feature=res[2],
                                                   value=res[3], measurement=res[4]))
            # session.add_all(obj_results)
            # session.commit()
            print('{} has finished'.format(feature_name))

        return

    except Exception as e:
        print(e)
        exit(-1)


def main(area, res):

    LOS_ANGELES = {
        'OSM': LOS_ANGELES_OSM,
        'BOUNDING_BOX': LOS_ANGELES_BOUNDING_BOX,
        'GRID_OBJ_500': LosAngeles500mGrid,
        'GRID_OBJ_1000': LosAngeles1000mGrid,
        'GEO_FEATURE_OBJ_500': LosAngeles500mGridGeoFeature,
        'GEO_FEATURE_OBJ_1000': LosAngeles500mGridGeoFeature,
    }

    conf = dict()

    if area == 'los_angeles':
        conf['AREA'] = area
        conf['BOUNDING_BOX'] = LOS_ANGELES['BOUNDING_BOX']
        conf['OSM'] = LOS_ANGELES['OSM']
        conf['GRID_OBJ'] = LOS_ANGELES[f'GRID_OBJ_{res}']
        conf['GEO_FEATURE_OBJ'] = LOS_ANGELES[f'GEO_FEATURE_OBJ_{res}']
    else:
        pass

    """ !!! Be careful, create table would overwrite the original table """
    # create_table(conf['GEO_FEATURE_OBJ'])
    compute_features_from_osm(conf)
