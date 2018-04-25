# Getting Started
Connections to Beakers may be established from the command line or programmatically. To establish a
connection from the command line, run ```./client.sh (<host>:<port>)?``` and type ```help``` at the 
prompt for a description of commands. If no host and port is specified, then ```localhost:9090``` is 
assumed. To establish a connection programmatically, refer to the following example or to the 
[documentation][1] for more information.

```scala
import beaker.client._
val client = Client("localhost", 9090)

// Read.
client.get("x")

// Update.
client.put("x", "hello")
client.get("x")

// Compare-and-Set.
client.cas(Map("x" -> 1L), Map("x" -> "goodbye"))
client.get("x")

// Asynchronous Iteration.
client.foreach(println)

// Cleanup.
client.close()
```

[1]: http://www.javadoc.io/doc/com.madavan/beaker-client_2.12/1.0.3