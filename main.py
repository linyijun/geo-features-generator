from geo_generator_buffer_based import gen_buffers
from geo_generator_grid_based import gen_grids
from geo_generator_buffer_based import gen_geo_features
import geo_generator_buffer_based
import geo_generator_grid_based


def main(task):

    if task == 'gen_grids':
        gen_grids.main('los_angeles', 500)
        gen_grids.main('los_angeles', 1000)
        gen_grids.main('los_angeles', 5000)

    if task == 'gen_buffers':
        gen_buffers.main('los_angeles', 'fishnet')
        gen_buffers.main('los_angeles', 'epa')

    if task == 'gen_geo_features_buffer_based':
        geo_generator_buffer_based.gen_geo_features.main('los_angeles', 'fishnet')
        geo_generator_buffer_based.gen_geo_features.main('los_angeles', 'epa')

    if task == 'gen_geo_features_grid_based':
        geo_generator_buffer_based.gen_geo_features.main('los_angeles', 500)
        geo_generator_buffer_based.gen_geo_features.main('los_angeles', 1000)

    return 0


if __name__ == '__main__':
    main('gen_buffers')
    main('gen_geo_features_buffer_based')
