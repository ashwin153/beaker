# Client
```scala
import beaker.client._
val client = Client("localhost", 9090)

// Update.
client.put("x" -> "hello")
client.get("x")

// Compare-and-set.
client.cas(Map("x" -> 1L), Map("x" -> "goodbye"))
client.get("x")

// Iteration.
client.foreach(println)

// Cleanup.
client.close()
```