package beaker.core

import beaker.core.protobuf._
import beaker.cluster
import beaker.cluster.Address

import io.grpc.{Channel, ManagedChannel, ManagedChannelBuilder}

/**
 * An internal, Beaker implementation.
 */
object Internal {

  /**
   * An internal, Beaker service.
   */
  case object Service extends cluster.Service[Internal.Client] {

    override def connect(address: Address): Client =
      Internal.Client(ManagedChannelBuilder.forAddress(address.host, address.port).build())

    override def disconnect(client: Internal.Client): Unit =
      client.channel.shutdown()

  }

  /**
   * An internal, Beaker client. Beakers use the client to communicate with each other. Supports
   * operations that facilitate consensus, which are not safe to be made externally visible.
   *
   * @param channel Underlying [[Channel]].
   */
  case class Client(channel: ManagedChannel) {

    private[this] val underlying = BeakerGrpc.blockingStub(this.channel)

    /**
     * Returns the latest known revision of each key.
     *
     * @param keys Keys to get.
     * @return Revision of each key.
     */
    def get(keys: Set[Key]): Map[Key, Revision] =
      this.underlying.get(Keys(keys.toSeq)).entries

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
      this.underlying.prepare(proposal)

    /**
     * Requests a vote for a proposal. Beakers cast a vote for a proposal if and only if a promise has
     * not been made to a newer proposal.
     *
     * @param proposal Proposal to accept.
     * @return Whether or not the transaction was accepted.
     */
    def accept(proposal: Proposal): Unit =
      this.underlying.accept(proposal)

    /**
     * Casts a vote for a proposal. Beakers commit the transactions and repairs of a proposal once a
     * quorum of beakers vote for it.
     *
     * @param proposal Proposal to learn.
     */
    def learn(proposal: Proposal): Unit =
      this.underlying.learn(proposal)

  }

}
