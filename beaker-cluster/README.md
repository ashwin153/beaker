# Cluster
Cluster provides tools to build, deploy, and connect to distributed services.

## Discovery
In practical systems, the members of a cluster are constantly changing as the cluster grows and
shrinks in size. Furthermore, members may fail arbitrarily. In the event of failure, members should
be automatically removed from the cluster. In this section, we describe how clients *dynamically*
discover the members of a cluster.

[1]: https://zookeeper.apache.org/
