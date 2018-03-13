# Ammonite
jar_library(name='ammonite', jars=[
    jar(org='com.lihaoyi', name='ammonite_2.12.1', rev='1.0.2'),
])

# ANTLR
jar_library(name='antlr', jars=[
    jar(org='org.antlr', name='antlr4', rev='4.7'),
    jar(org='org.antlr', name='antlr4-runtime', rev='4.7'),
])

# C3P0
jar_library(name='c3p0', jars=[
    jar(org='c3p0', name='c3p0', rev='0.9.1.2')
])

# Caffeine
jar_library(name='caffeine', jars=[
    jar(org='com.github.ben-manes.caffeine', name='caffeine', rev='2.5.5'),
])

# Config
jar_library(name='config', jars=[
    jar(org='com.typesafe', name='config', rev='1.3.1'),
])

# Curator
jar_library(name='curator', dependencies=[':zookeeper'], jars=[
    jar(org='org.apache.curator', name='curator-framework', rev='4.0.0', excludes=[exclude('org.apache.zookeeper')]),
    jar(org='org.apache.curator', name='curator-recipes', rev='4.0.0', excludes=[exclude('org.apache.zookeeper')]),
])

# Curator Test
jar_library(name='curator-test', jars=[
    jar(org='org.apache.curator', name='curator-test', rev='4.0.0'),
])

# Jedis
jar_library(name='jedis', jars=[
    jar(org='redis.clients', name='jedis', rev='2.9.0'),
])

# JUnit
jar_library(name='junit', jars=[
    jar(org='junit', name='junit', rev='4.12'),
    jar(org='org.pantsbuild', name='junit-runner-annotations', rev='0.0.17'),
])

# Mockito
jar_library(name='mockito', jars=[
    jar(org='org.mockito', name='mockito-core', rev='2.7.11'),
])

# MySQL
jar_library(name='mysql', jars=[
    jar(org='mysql', name='mysql-connector-java', rev='6.0.6'),
])

# PostgreSQL
jar_library(name='postgres', jars=[
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

# Scalac
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

# Scalameter
jar_library(name='scalameter', jars=[
    jar(org='com.storm-enroute', name='scalameter_2.12', rev='0.8.2')
])

# Scalatest
jar_library(name='scalatest', dependencies=[':scala-library', ':scala-reflect'], jars=[
    jar(org='org.scalatest', name='scalatest_2.12', rev='3.0.0', excludes=[exclude('org.scala-lang')]),
])

# Shapeless
jar_library(name='shapeless', jars=[
    jar(org='com.chuusai', name='shapeless_2.12', rev='2.3.2'),
])

# SLF4J
jar_library(name='slf4j-simple', jars=[
    jar(org='org.slf4j', name='slf4j-simple', rev='1.7.25'),
])

# Spray JSON
jar_library(name='spray-json', jars=[
    jar(org='io.spray', name='spray-json_2.12', rev='1.3.3'),
])

# Thrift
jar_library(name='thrift', jars=[
    jar(org='org.apache.thrift', name='libthrift', rev='0.11.0'),
])

# ZooKeeper
jar_library(name='zookeeper', jars=[
    jar(org='org.apache.zookeeper', name='zookeeper', rev='3.4.10'),
])
