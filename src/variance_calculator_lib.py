def prepare_shapes(routes, shapes, trips):
    # Explanation: indices get lost in the merge, so we need to dup them
    routes.insert(0, 'route_index', routes['route_id'])
    trips.insert(0, 'shape_index', trips['shape_id'])

    # Join
    df = routes.set_index('route_id').join(trips.set_index('route_id'))
    df.drop_duplicates(['route_index', 'shape_index'], inplace=True)
    df = df.set_index('shape_id').join(shapes.set_index('shape_id'))
    df = df[['route_index', 'shape_index', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon', 'shape_dist_traveled',
             'route_short_name', 'route_long_name']]
    return df
