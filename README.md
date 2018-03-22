![Logo](https://github.com/ashwin153/beaker/blob/master/beaker-assets/images/beaker-banner.png)
---
[![Build Status](https://travis-ci.org/ashwin153/beaker.svg?branch=master)][1]
[![Maven Central](https://img.shields.io/maven-central/v/com.madavan/beaker-server_2.12.svg)][2]
[![Docker](https://img.shields.io/docker/build/ashwin153/beaker.svg)][3]

Beaker is a distributed, transactional key-value store. Beaker is ```N/2``` fault tolerant but 
assumes that failures are fail-stop and that messages are received in order. Beaker is strongly 
consistent; conflicting transactions are linearized. Beaker features monotonic reads, 
read-your-write consistency, and tunable availability and partition tolerance.

# Structure
```
# Database
beaker/                             https://github.com/ashwin153/beaker
+---beaker-assets/                  Documentation, results, and graphics.
+---beaker-benchmark/               Performance tests.
+---beaker-client/                  Client library.
+---beaker-common/                  Tools.
+---beaker-server/                  Database server.
+---build-support/                  Pants plugins and configuration.
```

# Requirements
- Java 1.8 
- Python 2.7 (build-only) 
- Scala 2.12 

[1]: https://travis-ci.org/ashwin153/beaker
[2]: https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.madavan%22
[3]: https://hub.docker.com/r/ashwin153/beaker/
