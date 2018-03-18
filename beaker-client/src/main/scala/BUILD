scala_library(
    name='scala',
    sources=rglobs('*.scala'),
    dependencies=[
        'beaker-server/src/main/protobuf',
        '3rdparty/jvm:grpc-netty',
    ],
    provides=artifact(
        org='com.madavan',
        name='beaker-client_2.12',
        repo=public,
        publication_metadata=describe,
    ),
)


