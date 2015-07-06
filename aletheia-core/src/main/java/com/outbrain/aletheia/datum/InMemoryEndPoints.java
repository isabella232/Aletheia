package com.outbrain.aletheia.datum;

import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A helper class to easily manipulate {@link InMemoryEndPoint}s.
 */
public final class InMemoryEndPoints {

  private static final ConcurrentHashMap<String, InMemoryEndPoint> byName = new ConcurrentHashMap<>();

  private static int countDatums(final InMemoryEndPoint endPointName) {
    int drainSize = 0;
    if (endPointName != null) {
      for (final Iterable<byte[]> bytes : endPointName.getData().values()) {
        drainSize += Lists.newArrayList(bytes).size();
      }
    }
    return drainSize;
  }

  private static List<byte[]> drainData(final InMemoryEndPoint endPointName) {

    final LinkedList<byte[]> drainedData = Lists.newLinkedList();

    final int drainSize = countDatums(endPointName);

    for (int i = 0; i < drainSize; i++) {
      drainedData.addLast(endPointName.fetch());
    }

    return drainedData;
  }

  private static List<byte[]> drainData(final String endPointName) {
    return drainData(byName.get(endPointName));
  }

  /**
   * Drains the {@link InMemoryEndPoint} associated with a given source name, into a fresh {@link InMemoryEndPoint} instance.
   *
   * @param endPointName the name of the endpoint to be drained.
   * @return an {@link InMemoryEndPoint} holding the drained data.
   */
  public static InMemoryEndPoint drain(final String endPointName) {
    return new InMemoryEndPoint("drained_from_" + endPointName, drainData(endPointName));
  }

  /**
   * Creates a {@link InMemoryEndPoint} with the specified size, and associates it with the given name.
   *
   * @param endPointName  the name used to register the new endpoint
   * @param size the size of the registered endpoint
   * @return the endpoint registered
   */
  public static InMemoryEndPoint register(final String endPointName, final int size) {
    final InMemoryEndPoint newInMemoryEndPoint = new InMemoryEndPoint(endPointName, size);
    final InMemoryEndPoint existingMemoryEndPoint = byName.putIfAbsent(endPointName, newInMemoryEndPoint);
    return existingMemoryEndPoint != null ? existingMemoryEndPoint : newInMemoryEndPoint;
  }

  /**
   * Retrieves the {@link InMemoryEndPoint} associated with a given name.
   * @param endPointName the name of the endpoint to retrieve
   * @return the retrieved instance
   */
  public static InMemoryEndPoint get(final String endPointName) {
    return byName.get(endPointName);
  }


  /**
   * Clears the state of registered {@link InMemoryEndPoint}s/
   */
  public static void clearAll() {
    for (final InMemoryEndPoint inMemoryEndPoint : byName.values()) {
      drainData(inMemoryEndPoint);
    }
  }

}
