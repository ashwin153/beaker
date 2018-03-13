package caustic.cluster

/**
 * A distributed service.
 */
trait Service[Client] {

  /**
   * Constructs a client connected to the address.
   *
   * @param address Address to connect to.
   * @return Connected client.
   */
  def connect(address: Address): Client

  /**
   * Disconnects the client.
   *
   * @param client Client to disconnect.
   */
  def disconnect(client: Client): Unit

}