package caustic.cluster

import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import java.io.Closeable
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Random, Try}

/**
 * A collection of servers.
 */
trait Cluster[C] extends Closeable {

  /**
   * Returns the underlying services.
   *
   * @return Underlying service.
   */
  def service: Service[C]

  /**
   * Returns the known members of the cluster.
   *
   * @return Current members.
   */
  def members: Set[Address]

  /**
   * Adds the server to the cluster.
   *
   * @param instance Server address.
   */
  def join(instance: Address): Unit

  /**
   * Removes the instance from the cluster.
   *
   * @param instance Server address.
   */
  def leave(instance: Address): Unit

  /**
   * Returns the number of members.
   *
   * @return Number of members.
   */
  final def size: Int = this.members.size

  /**
   * Performs the request on all members of the cluster in parallel and returns their responses.
   *
   * @param request Request to perform.
   * @return Collection of responses.
   */
  final def broadcast[R](request: C => R): Seq[Try[R]] = {
    val client = this.members.map(service.connect).toSeq
    val result = client.par.map(c => Try(request(c))).seq
    client.foreach(this.service.disconnect)
    result
  }

  /**
   * Performs the request on all members of the cluster in parallel and returns their responses
   * if and only if a majority of the requests were successful.
   *
   * @param request Request to perform.
   * @return Collection of responses.
   */
  final def quorum[R](request: C => R): Try[Seq[R]] = {
    Try(broadcast(request).filter(_.isSuccess).map(_.get)).filter(_.size >= this.size / 2 + 1)
  }

  /**
   * Performs the request on a randomly chosen member of the cluster and returns the response.
   *
   * @param request Request to perform.
   * @tparam R
   * @return Collection of responses.
   */
  final def random[R](request: C => R): Try[R] = {
    val client = service.connect(Random.shuffle(this.members).head)
    val result = Try(request(client))
    service.disconnect(client)
    result
  }

}

object Cluster {

  /**
   * An in-memory, collection of instances. Useful for testing.
   *
   * @param buffer Current members.
   */
  case class Static[C](
    service: Service[C],
    buffer: mutable.Set[Address]
  ) extends Cluster[C] {

    override def members: Set[Address] = this.buffer.toSet
    override def join(instance: Address): Unit = this.buffer += instance
    override def leave(instance: Address): Unit = this.buffer -= instance
    override def close(): Unit = ()

  }

  object Static {

    /**
     * Constructs an empty static cluster.
     *
     * @return Empty static cluster.
     */
    def empty[C](service: Service[C]): Cluster.Static[C] = {
      new Cluster.Static[C](service, mutable.Set.empty[Address])
    }

    /**
     * Constructs a static cluster with the specified initial instances.
     *
     * @param initial Initial instances.
     *
     * @return Initialized static cluster.
     */
    def apply[C](service: Service[C], initial: Address*): Cluster.Static[C] = {
      val cluster = Cluster.Static.empty(service)
      initial.foreach(cluster.join)
      cluster
    }
  }

  /**
   * A ZooKeeper-managed cluster. When an instance joins or leaves the cluster, the instance is
   * created or deleted in ZooKeeper. Instances are automatically deleted during disruptions in
   * ZooKeeper connectivity, because they are created as ephemeral sequential nodes, and they are
   * automatically recreated when connectivity is restored. Each cluster watches for when instances
   * are created and deleted in ZooKeeper and updates its own view of the cluster accordingly.
   *
   * @param curator An un-started [[CuratorFramework]].
   * @param created Instances created by this cluster.
   * @param awareOf Instances known to this cluster.
   */
  case class Dynamic[C](
    service: Service[C],
    curator: CuratorFramework,
    created: mutable.Map[Address, String] = mutable.Map.empty,
    awareOf: mutable.Map[String, Address] = mutable.Map.empty
  ) extends Cluster[C] {

    // Re-register all created addresses after disruptions in ZooKeeper connectivity.
    this.curator.getConnectionStateListenable.addListener((_, s) =>
      if (s == ConnectionState.CONNECTED || s == ConnectionState.RECONNECTED) {
        this.created.keys.foreach(join)
      }
    )

    // Construct a ZooKeeper cache to monitor instance changes.
    private val cache = new PathChildrenCache(this.curator, "/" + this.curator.getNamespace, false)
    this.cache.getListenable.addListener((_, e) =>
      if (e.getType == Type.CHILD_ADDED || e.getType == Type.CHILD_UPDATED) {
        val address = Address(e.getData.getData)
        this.awareOf += e.getData.getPath -> address
      } else if (e.getType == Type.CHILD_REMOVED) {
        this.awareOf.remove(e.getData.getPath)
      }
    )

    // Connect to ZooKeeper.
    this.cache.start()
    this.curator.start()

    override def close(): Unit = {
      this.cache.close()
      this.curator.close()
    }

    override def members: Set[Address] = {
      this.awareOf.values.toSet
    }

    override def join(instance: Address): Unit = {
      // Remove the instance if it already exists.
      leave(instance)

      // Announce the instance in ZooKeeper.
      this.curator.blockUntilConnected()
      this.created += instance -> this.curator.create()
        .creatingParentContainersIfNeeded()
        .withProtection()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath("/instance", instance.toBytes)
    }

    override def leave(instance: Address): Unit = {
      this.created.remove(instance) foreach { p =>
        this.curator.blockUntilConnected()
        this.curator.delete().forPath(p)
      }
    }

  }

  object Dynamic {

    /**
     * A ZooKeeper configuration.
     *
     * @param zookeeper Connection string. (eg. "localhost:3192,localhost:2811")
     * @param namespace Directory in which instances are registred.
     * @param connectionTimeout ZooKeeper connection timeout.
     * @param sessionTimeout ZooKeeper session timeout.
     */
    case class Config(
      zookeeper: String = "localhost:2811",
      namespace: String = "services",
      connectionTimeout: Duration = 15 seconds,
      sessionTimeout: Duration = 60 seconds
    )

    /**
     * Constructs a dyanmic cluster from the provided configuration.
     *
     * @param config Configuration.
     * @return Configured dynamic cluster.
     */
    def apply[C](service: Service[C], config: Config): Cluster.Dynamic[C] =
      Cluster.Dynamic[C](service, CuratorFrameworkFactory.builder()
        .connectString(config.zookeeper)
        .namespace(config.namespace)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .connectionTimeoutMs(config.connectionTimeout.toMillis.toInt)
        .sessionTimeoutMs(config.sessionTimeout.toMillis.toInt)
        .build())

  }


}