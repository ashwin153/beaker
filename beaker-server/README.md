# Getting Started
An __instance__ may be started and run using [Docker][2]. Refer to the [reference configuration][3] 
for information about the various configuration parameters and their default values. This 
configuration may be optionally override by providing a static configuration file, 
```application.conf```, or by explicitly overriding parameters from the command line.

```
docker run -d \
  -p ${external port}:${internal port} \
  -v ${/path/to/application.conf}:/beaker/beaker-server/src/main/resources/application.conf \
  ashwin153/beaker \
  ./pants run beaker-server/src/main/scala:bin \
  -- -Dbeaker.database.sql.password=${password}
```

# Background
A __database__ is a key-value store. Databases map keys to versioned values, called __revisions__. 
Revisions are uniquely identified and totally ordered by their version. Keys may be *read* or
*written* from a database. A __transaction__ depends on the versions of a set of keys, called its 
*readset*, and changes the values of a set of keys, called its *writeset*. Transactions may be 
*committed* on a database if and only if the versions they depend on are greater than or equal to 
their versions in the database. Revisions are monotonic; if a transaction changes a key for which 
there exists a newer revision in the database, the modification is discarded. This ensures that 
transactions cannot undo the effect of other transactions. We say that a transaction ```A``` 
*conflicts with* ```B``` if either reads or writes a key that the other writes. 

A distributed, transactional database is a collection of __beakers__. Each beaker maintains its own 
replica of the database. Beakers use an __executor__ to linearize conflicting reads, writes, and 
commits locally, and a variation of [Generalized Paxos][1] to linearize conflicting 
transactions globally with several desirable properties. First, beakers may simultaneously commit 
non-conflicting transactions. Second, beakers automatically repair replicas that have stale 
revisions. Third, beakers may safely commit transactions as long as at least a majority is 
operational.

# Consensus
Beakers reach consensus on __proposals__. A proposal is a collection of non-conflicting 
transactions. These transactions may conditionally apply changes or unconditionally repair stale
revisions. Proposals are uniquely identified and totally ordered by a __ballot__ number. We say that
a proposal ```A``` *conflicts with* ```B``` if a transaction that is applied by ```A``` conflicts 
with a transaction that is applied by ```B```. We say that a proposal ```A``` is *older than* 
```B``` if ```A``` conflicts with ```B``` and ```A``` has a lower ballot than ```B```. We say that
a proposal ```A``` *matches* ```B``` if ```A``` applies the same transactions as ```B```. Proposals 
```A``` and ```B``` may be *merged* by taking the maximum of their ballots, combining the 
transactions they apply choosing the transactions in the newer proposal in the case of conflicts, 
and combining their repairs choosing highest revision changes in the case of duplicates. 

The leader for a proposal ```P``` first *prepares* ```P``` on a majority of beakers. If a beaker has 
not made a promise to a newer proposal, it responds with a __promise__ not to accept any proposal 
that conflicts with the proposal it returns that has a lower ballot than ```P```. If a beaker has 
already accepted proposals older than ```P```, merges them together and returns the result. 
Otherwise, it returns the proposal with a zero ballot. If the leader does not receive a majority of 
promises, it retries with a higher ballot. Otherwise, it merges the returned promises into a single 
proposal ```P'```. If ```P``` does not match ```P'```, it retries with ```P'```. Otherwise, the 
leader *gets* the latest versions of the keys that are read by ```P``` from a majority of beakers. 
The leader discards all transactions in ```P``` that cannot be committed given the latest versions, 
and sets its repairs to the latest revisions of keys that are read - but not written - by ```P``` 
for which the beakers disagree on their version. The leader then requests a majority of beakers to
*accept* ```P```. A beaker accepts a proposal if it has not promised not to. When a beaker accepts a 
proposal, it discards all older accepted proposals and broadcasts a __vote__ for it. We say that a 
proposal is *accepted* if a majority of beakers vote for it. A beaker *learns* a proposal once a 
majority of beakers vote for it. If a beaker learns a proposal, it commits its transactions and 
repairs on its replica of the database.

## Correctness
The proof of correctness relies on the assumption of *connectivity*, beakers are always connected to 
all of their non-faulty peers, and the fact of *quorum intersection*, any majority of beakers will 
contain at least one beaker in common.

__Liveness.__ An accepted proposal ```A``` will eventually be learned. __Proof.__ By quorum
intersection, at least one promise will contain ```A```. Therefore, ```A``` must be proposed enough
beakers learn ```A``` such that ```A``` is no longer accepted by a majority. By assumption of
connectivity, if any beaker learns a proposal then all beakers will eventually learn it.

__Linearizability.__ If a proposal ```A``` is accepted, then any conflicting proposal ```B``` that
is accepted after ```A``` will be learned after ```A```. __Proof.__ Because ```A``` was accepted
before ```B```, the majority that accepted ```A``` before ```B``` will vote for ```A``` before
```B```. Because messages are delivered in order, ```A``` will be learned before ```B```.

__Commutativity.__ Let ```R``` denote the repairs for an accepted proposal ```A```. Any accepted
proposal ```B``` that conflicts with ```A + R``` but not ```A``` commutes with ```A + R```.
__Proof.__ Because ```B``` conflicts with ```A + R``` but not ```A```, ```B``` must read a key
```k``` that is read by ```A```. By linearizability, ```B``` must read the latest version of
```k``` because ```B``` is accepted. Suppose that ```B``` is committed first. Because ```B``` reads 
and does not write ```k```, ```A + R``` can still be committed. Suppose that ```A + R``` is 
committed first. Because ```A + R``` writes the latest version of ```k``` and ```B``` reads the 
latest version, ```B``` can still be committed.

__Consistency.__ If a proposal ```A``` is accepted, it can be committed. __Proof.__ Suppose there
exists a transaction that cannot be committed. Then, the transaction must read a key for which there
exists a newer version. This implies that there exists a proposal ```B``` that was accepted after
but learned before ```A``` that changes a key ```k``` that is read by ```A```. By linearizability,
 ```B``` cannot conflict with ```A```. Therefore, ```B``` must repair ```k```. By commutativity,
```A``` may still be committed.

# Reconfiguration
Each beaker is required to be connected to a majority of non-faulty peers in order to guarantee 
correctness. However, this correctness condition is only valid when the cluster is static. In
practical systems, beakers may join or leave the cluster arbitrarily as the cluster grows or shrinks 
in size. In this section, we describe how *fresh* beakers are *bootstrapped* when they join an 
existing cluster. When a fresh beaker joins a cluster, its database is initially empty. In order to 
guarantee correctness, its database must be immediately populated with the latest revision of every
key-value pair. Otherwise, if ```N + 1``` fresh beakers join a cluster of size ```N``` it 
is possible for a quorum to consist entirely of fresh beakers. 

A naive solution might be for the fresh beaker to propose a read-only transaction that depends on
the initial revision of every key-value pair in the database and conflicts with every other
proposal. Then, the fresh beaker would automatically repair itself in the process of committing this
transaction. However, this is infeasible in practical systems because databases may contain
arbitrarily many key-value pairs. This approach would inevitably saturate the network because for a
database of size ```D``` such a proposal consumes ```D * (3 * N / 2 + N * N)``` in bandwidth. 
Furthermore, it prevents any proposals from being accepted in the interim.

We can improve this solution by decoupling bootstrapping and consensus. A fresh beaker joins the 
cluster as a partial member; it learns proposals, but is not involved in consensus prepare them. 
Therefore, a partial member will vote for every proposal. A proposal is learned when a majority of 
full members vote for it. The fresh beaker scans a quorum of full members, and repairs its own
replica of the database with the returned values. In this manner, it is guaranteed to have the
latest value of every key in the database. It then joins the cluster as a full member. This approach 
consumes just ```D * N / 2``` in bandwidth and permits concurrent proposals.

# Caching
A __cache__ is a write-through database. Because dependencies are validated on commit, keys may be
speculatively read from cache without sacrificing consistency. Beaker supports multi-level cache
hierarchies over the underlying database. Revisions may be *fetched* or *updated* in cache. Cache
coherency is maintained by updating the cache whenever transactions commit successfully or otherwise
on the underlying database.

[1]: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-33.pdf
[2]: https://hub.docker.com/r/ashwin153/beaker/
[3]: https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf