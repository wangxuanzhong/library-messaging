package com.syswin.library.messaging.embedded;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class EmbeddedMessageQueue implements MessageQueue {

  private static final Queue<Consumer<String>> EMPTY_QUEUE = new LinkedList<>();

  private final Map<String, Queue<Consumer<String>>> messageListeners = new ConcurrentHashMap<>();

  @Override
  public void send(String topic, String message) {
    messageListeners.getOrDefault(topic, EMPTY_QUEUE).forEach(listener -> listener.accept(message));
  }


  @Override
  public void subscribe(String topic, Consumer<String> messageListener) {
    messageListeners.computeIfAbsent(topic, key -> new ConcurrentLinkedQueue<>()).offer(messageListener);
  }
}
