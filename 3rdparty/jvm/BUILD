####################################################################################################
#                                             Compile                                              #
####################################################################################################
# C3P0
jar_library(name='c3p0', jars=[
    jar(org='c3p0', name='c3p0', rev='0.9.1.2')
])

# Caffeine
jar_library(name='caffeine', jars=[
    jar(org='com.github.ben-manes.caffeine', name='caffeine', rev='2.5.5'),
])

# gRPC Netty
jar_library(name='grpc-netty', jars=[
    jar(org='io.grpc', name='grpc-netty', rev='1.10.0'),
    jar(org='io.netty', name='netty-tcnative-boringssl-static', rev='2.0.7.Final'),
    jar(org='io.netty', name='netty-handler', rev='4.1.17.Final'),
])

# Jedis
jar_library(name='jedis', jars=[
    jar(org='redis.clients', name='jedis', rev='2.9.0'),
])

# MySQL
jar_library(name='mysql', jars=[
    jar(org='mysql', name='mysql-connector-java', rev='6.0.6'),
])

# PostgreSQL
jar_library(name='postgresql', jars=[
    jar(org='postgresql', name='postgresql', rev='9.1-901.jdbc4'),
])

# Pureconfig
jar_library(name='pureconfig', jars=[
    jar(org='com.github.pureconfig', name='pureconfig_2.12', rev='0.8.0'),
])

# Rediscala
jar_library(name='rediscala', jars=[
    jar(org='com.github.etaty', name='rediscala_2.12', rev='1.8.0'),
])

# ScalaPB Runtime
jar_library(name='scalapb-runtime', jars = [
    jar(org='com.trueaccord.scalapb', name='scalapb-runtime_2.12', rev='0.6.7')
])

# SLF4J
jar_library(name='slf4j-simple', jars=[
    jar(org='org.slf4j', name='slf4j-simple', rev='1.7.25'),
])

####################################################################################################
#                                              Build                                               #
####################################################################################################
# Ammonite
jar_library(name='ammonite', jars=[
    jar(org='com.lihaoyi', name='ammonite_2.12.1', rev='1.0.5'),
])

# Protobuf
jar_library(name='protobuf', jars = [
    jar(org='com.google.protobuf', name='protobuf-java', rev='3.1.0'),
])

# Scala Compiler
jar_library(name='scala-compiler', dependencies=[':scala-library', ':scala-reflect'], jars=[
    jar(org='org.scala-lang', name='scala-compiler', rev='2.12.1'),
])

# Scala Library
jar_library(name='scala-library', jars=[
    jar(org='org.scala-lang', name='scala-library', rev='2.12.1'),
])

# Scala Reflection
jar_library(name='scala-reflect', jars=[
    jar(org='org.scala-lang', name='scala-reflect', rev='2.12.1', intransitive=True),
])

# ScalaPB gRPC
jar_library(name='scalapb-runtime-grpc', jars = [
    jar(org='com.trueaccord.scalapb', name='scalapb-runtime-grpc_2.12', rev='0.6.7')
])

# ScalaPB Compiler
jar_library(name='scalapbc', jars=[
    scala_jar(org='com.trueaccord.scalapb', name='scalapbc', rev='0.6.7'),
])

####################################################################################################
#                                               Test                                               #
####################################################################################################
# JUnit
jar_library(name='junit', jars=[
    jar(org='junit', name='junit', rev='4.12'),
    jar(org='org.pantsbuild', name='junit-runner-annotations', rev='0.0.17'),
])

# Mockito
jar_library(name='mockito', jars=[
    jar(org='org.mockito', name='mockito-core', rev='2.7.11'),
])

# Scalameter
jar_library(name='scalameter', jars=[
    jar(org='com.storm-enroute', name='scalameter_2.12', rev='0.8.2')
])

# Scalatest
jar_library(name='scalatest', dependencies=[':scala-library', ':scala-reflect'], jars=[
    jar(org='org.scalatest', name='scalatest_2.12', rev='3.0.0', excludes=[exclude('org.scala-lang')]),
])