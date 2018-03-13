scala_library(
    name='scala',
    sources=rglobs('*.scala'),
    dependencies=[
        'beaker-core/src/main/thrift',
        '3rdparty/jvm:curator',
        '3rdparty/jvm:pureconfig',
        '3rdparty/jvm:slf4j-simple',
        '3rdparty/jvm:thrift',
    ],
    provides=artifact(
        org='com.madavan',
        name='beaker-cluster_2.12',
        repo=public,
        publication_metadata=describe,
    ),
)


