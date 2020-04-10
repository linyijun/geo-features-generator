from geoalchemy2 import Geography
from sqlalchemy import func, cast

from data_models.buffer_model import *
from data_models.common_db import session
from data_models.location_model import *
from utils import create_table


def generate_buffers(config):

    buffer_size = config['BUFFER_SIZE']
    buffer_obj = config['BUFFER_OBJ']
    loc_obj = config['LOC_OBJ']

    try:
        for i in range(100, buffer_size + 100, 100):

            if 'epa' in config['AREA']:
                results = session.query(loc_obj.station_id, loc_obj.lon, loc_obj.lat,
                                        cast(func.ST_Buffer(cast(loc_obj.location, Geography), i), Geometry)).all()
            else:
                results = session.query(loc_obj.gid, loc_obj.lon, loc_obj.lat,
                                        cast(func.ST_Buffer(cast(loc_obj.location, Geography), i), Geometry)).all()

            obj_results = []
            for res in results:
                obj_results.append(buffer_obj(gid=res[0], lon=res[1], lat=res[2], buffer_size=i, buffer=res[3]))
            session.add_all(obj_results)
            session.commit()
            print('Buffer {} has finished'.format(i))
        return

    except Exception as e:
        print(e)
        exit(-1)


def main(area, tar):

    LOS_ANGELES = {
        'BUFFER_SIZE': 3000,
        'BUFFER_OBJ_EPA': LosAngelesEPA3000mBuffer,
        'LOC_OBJ_EPA': LosAngelesEPASensorLocations,
        'BUFFER_OBJ_FISHNET': LosAngelesFishnet3000mBuffer,
        'LOC_OBJ_FISHNET': LosAngelesFishnet,
    }

    conf = dict()

    if area == 'los_angeles':
        conf['AREA'] = area + '_' + tar
        conf['BUFFER_SIZE'] = LOS_ANGELES['BUFFER_SIZE']
        conf['BUFFER_OBJ'] = LOS_ANGELES[f'BUFFER_OBJ_{tar.upper()}']
        conf['LOC_OBJ'] = LOS_ANGELES[f'LOC_OBJ_{tar.upper()}']
    else:
        pass

    """ !!! Be careful, create table would overwrite the original table """
    create_table(conf['BUFFER_OBJ'])
    generate_buffers(conf)


