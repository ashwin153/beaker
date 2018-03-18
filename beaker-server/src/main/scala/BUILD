scala_library(
    name='scala',
    sources=rglobs('*.scala'),
    dependencies=[
        'beaker-client/src/main/scala',
        'beaker-common/src/main/scala',
        'beaker-server/src/main/protobuf',
        'beaker-server/src/main/resources',
        '3rdparty/jvm:c3p0',
        '3rdparty/jvm:caffeine',
        '3rdparty/jvm:curator',
        '3rdparty/jvm:jedis',
        scoped('3rdparty/jvm:mysql', scope='runtime'),
        scoped('3rdparty/jvm:postgres', scope='runtime'),
        '3rdparty/jvm:pureconfig',
    ],
    provides=artifact(
        org='com.madavan',
        name='beaker-core_2.12',
        repo=public,
        publication_metadata=describe,
    ),
)
