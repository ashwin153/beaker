![Logo](https://github.com/ashwin153/beaker/blob/master/beaker-assets/images/banner.png)
---
[![Build Status](https://travis-ci.org/ashwin153/beaker.svg?branch=master)][1]
[![Docker](https://img.shields.io/docker/build/ashwin153/beaker.svg)][2]

Beaker is a distributed, transactional key-value store. Beaker is ```N / 2``` fault tolerant but 
assumes that failures are fail-stop and that messages are received in order. Beaker is strongly 
consistent; conflicting transactions are linearized. Beaker features monotonic reads, 
read-your-write consistency, and tunable availability and partition tolerance.

# Structure
```
# Database
beaker/                             https://github.com/ashwin153/beaker
+---beaker-assets/                  Documentation, results, and graphics.
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
[2]: https://hub.docker.com/r/ashwin153/beaker/
