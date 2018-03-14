![Logo](https://github.com/ashwin153/beaker/blob/master/beaker-assets/images/banner.png)
---
[![Build Status](https://travis-ci.org/ashwin153/beaker.svg?branch=master)][1]
[![Docker](https://img.shields.io/docker/build/ashwin153/beaker.svg)][2]

Beaker is a distributed, transactional key-value store that is consistent and available. Beaker is
```N / 2``` fault tolerant but assumes that failures are fail-stop, messages are received in order,
and network partitions never occur. Beaker is strongly consistent; conflicting transactions are
linearized but non-conflicting transactions are not. Beaker features monotonic reads and 
read-your-write consistency.

# Structure
```
# Database
beaker/                             https://github.com/ashwin153/beaker
+---beaker-assets/                  Documentation, results, and graphics.
+---beaker-benchmark/               Performance tests.
+---beaker-cluster/                 Service discovery.
+---beaker-common/                  Tools.
+---beaker-core/                    Distributed database.
+---build-support/                  Pants plugins and configuration.

# YCSB Benchmarks
ycsb/                               https://github.com/ashwin153/YCSB
+---caustic                         Caustic integration.
```

# Requirements
- Java 1.8 
- Python 2.7 (build-only) 
- Scala 2.12 

[1]: https://travis-ci.org/ashwin153/beaker
[2]: https://hub.docker.com/r/ashwin153/beaker/
