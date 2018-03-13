package caustic.beaker

import caustic.beaker.thrift._
import caustic.cluster
import caustic.cluster.Address
import caustic.cluster.protocol.Thrift

import scala.collection.JavaConverters._

/**
 * An internal, Beaker implementation.
 */
object Internal {

  /**
   * An internal, Beaker service.
   */
  case object Service extends cluster.Service[Internal.Client] {

    private val underlying = Thrift.Service(new thrift.Beaker.Client.Factory())

    override def connect(address: Address): Internal.Client =
      Internal.Client(this.underlying.connect(address))

    override def disconnect(client: Internal.Client): Unit =
      this.underlying.disconnect(client.underlying)

  }

  /**
   * An internal, Beaker client. Beakers use the client to communicate with each other. Supports
   * operations that facilitate consensus, which are not safe to be made externally visible.
   *
   * @param underlying Underlying Thrift client.
   */
  case class Client(underlying: Thrift.Client[thrift.Beaker.Client]) {

    /**
     * Returns the latest known revision of each key.
     *
     * @param keys Keys to get.
     * @return Revision of each key.
     */
    def get(keys: Set[Key]): Map[Key, Revision] =
      this.underlying.connection.get(keys.asJava).asScala.toMap

    /**
     * Makes a promise not to accept any proposal that conflicts with the proposal it returns and has
     * a lower ballot than the proposal it receives. If a promise has been made to a newer proposal,
     * its ballot is returned. If older proposals have already been accepted, they are merged together
     * and returned. Otherwise, it returns the proposal it receives with the default ballot.
     *
     * @param proposal Proposal to prepare.
     * @return Promised proposal.
     */
    def prepare(proposal: Proposal): Proposal =
      this.underlying.connection.prepare(proposal)

    /**
     * Casts a vote for a proposal if and only if a promise has not been made to a newer proposal.
     *
     * @param proposal Proposal to accept.
     * @return Whether or not the proposal was accepted.
     */
    def accept(proposal: Proposal): Unit =
      this.underlying.connection.accept(proposal)

    /**
     * Votes for a proposal. Beakers commit the transactions and repairs of a proposal once a majority
     * of beakers vote for it.
     *
     * @param proposal Proposal to learn.
     */
    def learn(proposal: Proposal): Unit =
      this.underlying.connection.learn(proposal)

  }

}
