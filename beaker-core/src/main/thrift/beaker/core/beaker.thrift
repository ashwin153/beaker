namespace * beaker.core.thrift

typedef string Key
typedef i64 Version
typedef string Value

/**
 * A versioned value. Revisions are uniquely-identified and totally-ordered by their version.
 * Revisions are monotonic; if a transaction changes a key for which there exists a newer revision,
 * the modification is discarded.
 *
 * @param version Version number.
 * @param value Value.
 */
struct Revision {
  1: Version version = 0,
  2: Value value
}

/**
 * A conditional update. Transactions depend on the versions of a set of keys, called its readset,
 * and update the revisions of a set of keys, called its writeset. Defaults to an empty transaction.
 *
 * @param depends Version dependencies.
 * @param updates Changed revisions.
 */
struct Transaction {
  1: map<Key, Version> depends = [],
  2: map<Key, Revision> changes = [],
}

/**
 * A monotonically-increasing, globally-unique sequence number. Defaults to the zero ballot.
 *
 * @param round Locally-unique number.
 * @param id Globally-unique number.
 */
struct Ballot {
  1: i32 round = 0,
  2: i32 id = 0
}

/**
 * A collection of non-conflicting transactions. These transactions may conditionally apply updates
 * or unconditionally repair stale revisions. Proposals are uniquely-identified and totally-ordered
 * by their ballot.
 *
 * @param ballot Ballot number.
 * @param commits Transactions to commit.
 * @param repairs Repairs to perform.
 */
struct Proposal {
  1: Ballot ballot,
  2: set<Transaction> applies = [],
  3: Transaction repairs = {},
}

/**
 * A distributed, transactional key-value store.
 */
service Beaker {

  /**
   * Returns the latest known revision of each key.
   *
   * @param keys Keys to get.
   * @return Revision of each key.
   */
  map<Key, Revision> get(1: set<Key> keys),

  /**
   * Conditionally applies the updates if and only if they depend on the latest versions.
   *
   * @param depends Dependencies.
   * @param updates Updates to apply.
   * @return Whether or not the updates were applied.
   */
  bool cas(1: map<Key, Version> depends, 2: map<Key, Value> updates),

  /**
   * Makes a promise not to accept any proposal that conflicts with the proposal it returns and has
   * a lower ballot than the proposal it receives. If a promise has been made to a newer proposal,
   * its ballot is returned. If older proposals have already been accepted, they are merged together
   * and returned. Otherwise, it returns the proposal it receives with the default ballot.
   *
   * @param proposal Proposal to prepare.
   * @return Promised proposal.
   */
  Proposal prepare(1: Proposal proposal),

  /**
   * Casts a vote for a proposal if and only if a promise has not been made to a newer proposal.
   *
   * @param proposal Proposal to accept.
   * @return Whether or not the proposal was accepted.
   */
  oneway void accept(1: Proposal proposal),

  /**
   * Votes for a proposal. Beakers commit the transactions and repairs of a proposal once a majority
   * of beakers vote for it.
   *
   * @param proposal Proposal to learn.
   */
  oneway void learn(1: Proposal proposal),

}