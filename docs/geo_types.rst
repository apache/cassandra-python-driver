DSE Geometry Types
==================
This section shows how to query and work with the geometric types provided by DSE.

These types are enabled implicitly by creating the Session from :class:`cassandra.cluster.Cluster`.
This module implicitly registers these types for use in the driver. This extension provides
some simple representative types in :mod:`cassandra.util` for inserting and retrieving data::

    from cassandra.cluster import Cluster
    from cassandra.util import Point, LineString, Polygon
    session = Cluster().connect()

    session.execute("INSERT INTO ks.geo (k, point, line, poly) VALUES (%s, %s, %s, %s)",
                    0, Point(1, 2), LineString(((1, 2), (3, 4))), Polygon(((1, 2), (3, 4), (5, 6))))

Queries returning geometric types return the :mod:`dse.util` types. Note that these can easily be used to construct
types from third-party libraries using the common attributes::

    from shapely.geometry import LineString
    shapely_linestrings = [LineString(res.line.coords) for res in session.execute("SELECT line FROM ks.geo")]

For prepared statements, shapely geometry types can be used interchangeably with the built-in types because their
defining attributes are the same::

    from shapely.geometry import Point
    prepared = session.prepare("UPDATE ks.geo SET point = ? WHERE k = ?")
    session.execute(prepared, (0, Point(1.2, 3.4)))

In order to use shapely types in a CQL-interpolated (non-prepared) query, one must update the encoder with those types, specifying
the same string encoder as set for the internal types::

    from cassandra import util
    from shapely.geometry import Point, LineString, Polygon

    encoder_func = session.encoder.get_encode_function(util.Point)
    for t in (Point, LineString, Polygon):
        session.encoder.add_or_override_mapping(t, encoder_func)

    session.execute("UPDATE ks.geo SET point = %s where k = %s", (0, Point(1.2, 3.4)))
