scala_library(
    name='scala',
    sources=rglobs('*.scala'),
    dependencies=[
        'beaker-cluster/src/main/scala',
        'beaker-common/src/main/scala',
        'beaker-core/src/main/thrift',
        '3rdparty/jvm:c3p0',
        '3rdparty/jvm:caffeine',
        '3rdparty/jvm:curator',
        scoped('3rdparty/jvm:mysql', scope='runtime'),
        scoped('3rdparty/jvm:postgres', scope='runtime'),
        '3rdparty/jvm:pureconfig',
        '3rdparty/jvm:jedis',
    ],
    provides=artifact(
        org='com.madavan',
        name='beaker-core_2.12',
        repo=public,
        publication_metadata=describe,
    ),
)
