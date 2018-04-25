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
        '3rdparty/jvm:grpc-netty',
        '3rdparty/jvm:jedis',
        scoped('3rdparty/jvm:mysql', scope='runtime'),
        scoped('3rdparty/jvm:postgresql', scope='runtime'),
        '3rdparty/jvm:pureconfig',
        '3rdparty/jvm:scala-logging',
        '3rdparty/jvm:slf4j-simple',
    ],
    provides=artifact(
        org='com.madavan',
        name='beaker-server_2.12',
        repo=public,
        publication_metadata=describe,
    ),
)

jvm_binary(
    name='bin',
    basename='beaker-server',
    source='beaker/server/Server.scala',
    main='beaker.server.Server',
    dependencies=[
        'beaker-server/src/main/scala',
    ],
)