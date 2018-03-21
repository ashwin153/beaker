# Tools
- MacBook Pro (2.6 GHz Intel Core i5; 16 GB 1600 MHz DDR3)
- [JProfiler][1]
- [Scalameter][2]

# Contention
- What happens to performance under contention? 

We may theoretically model the number retries required to successfully execute a transaction as a
[negative binomial distribution][3] in which the probability of success ```p``` is the *contention 
probability*. Therefore, ```A ~ 1 + NB(p, 1)``` is the distribution of total attempts. We 
may then use known results about the negative binomial distribution, to make predictions 
about the mean and variance of transaction execution latency under contention. 

What exactly is this mysterious contention probability ```p```? Consider two sets ```X``` and 
```Y``` each containing ```l``` integers selected uniformly at random from the set ```[0, n)```. 
With what probability do ```X``` and ```Y``` have at least one element in common? Equivalently, we 
may find the complement of the probability that ```X``` and ```Y``` have no elements in common. 
Intuitively, the probability that ```X``` and ```Y``` are disjoint is equal to the probability that 
```Y``` is drawn from the set ```[0, n) - X```. Therefore, the probability that ```X``` and ```Y``` 
have at least one element in common is ```contention(n, l) = 1 - C(n - l, l) / C(n, l)``` where 
```C(a, b)``` corresponds to the number of ways to choose ```b``` items from a set of size ```a```.

We may use this formula for the probability that two randomly generated sets of integers overlap to
define the contention probability ```p``` between two randomly generated transactions. Suppose two
transactions ```T1``` and ```T2``` each modify ```l``` keys chosen uniformly at random from a key
space of size ```n``` and are executed simultaneously. The probability that they will conflict is
exactly equal to the probability defined above! Crucially, defining contention probability in this
manner allows us to analyze performance under contention *relative to performance in isolation*
because both benchmarks were defined in terms of transaction size. 

Does this theoretical definition of contention probability match empirical results? To verify my
hypothesis, I constructed a Monte Carlo simulation in which two threads repeatedly generate 
transactions that randomly increment a set of ```l``` keys and execute them on an underlying 
database while monitoring the total number of failures. We would expect that for ```t``` threads,
```simulate(t, n, l)``` should produce ```contention(n, l) * t``` failures. We also require an a
dditional correctness criteria that the sum of all the values in the database should equal the total 
number of successful increments or ```l * (2t - simulate(t, n, l)```. I found both criteria to be 
empirically satisfied, and the reader is encouraged to verify my claims for themselves.

How can we apply what we know about contention probabilities and the negative binomial distribution
to determine the performance degradation of the system under contention? For example, suppose we 
have a key space of size ```1K``` and two contending processes that randomly mutate ```10``` keys.
This implies a contention probability of ```0.096```. Therefore, we would expect each transaction to 
require ```1 + p / (1 - p) = 1.106``` attempts and so the mean throughput of the system will be 
```1.106``` times less under contention.

Most practical use cases will have far more than two concurrently executing transactions. Can we
generalize this probabalistic argument to an arbitrary number of concurrently executing 
transactions? I confess that I lack the mathematical acumen to analytically derive the contention
probability in this generalized scenario. However, the contention probability can be numerically
determined by increasing the number of threads in the Monte Carlo simulation. The following charts
show how the contention probability and the average attempts grow with ```n``` and ```l```.

<img src="https://github.com/ashwin153/beaker/blob/master/beaker-assets/images/benchmark-contention.png" width="49%" style="float: left"/>
<img src="https://github.com/ashwin153/beaker/blob/master/beaker-assets/images/benchmark-attempts.png" width="49%" style="float: right"/>

[1]: https://www.ej-technologies.com/products/jprofiler/overview.html
[2]: https://scalameter.github.io/
[3]: https://en.wikipedia.org/wiki/Negative_binomial_distribution
