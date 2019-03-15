package com.syswin.library.messaging.embedded;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.invoke.MethodHandles;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedMessageQueue implements MessageQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Queue<Consumer<String>> EMPTY_QUEUE = new LinkedList<>();

  private final Map<String, Queue<Consumer<String>>> messageListeners = new ConcurrentHashMap<>();
  private final Map<String, BlockingQueue<String>> queues = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final long pollInterval;

  public EmbeddedMessageQueue() {
    this(100L);
  }

  public EmbeddedMessageQueue(long pollInterval) {
    this.pollInterval = pollInterval;
  }

  @Override
  public void start() {
    scheduledExecutor.scheduleWithFixedDelay(() ->
        queues.forEach((topic, queue) -> {
          String message = queue.poll();
          if (message != null) {
            messageListeners.getOrDefault(topic, EMPTY_QUEUE)
                .forEach(listener -> consumeSilently(message, listener));
          }
        }), pollInterval, pollInterval, MILLISECONDS);
  }

  @Override
  public void send(String topic, String message) {
    queues.computeIfAbsent(topic, k -> new LinkedBlockingQueue<>()).offer(message);
  }

  @Override
  public void subscribe(String topic, Consumer<String> messageListener) {
    messageListeners.computeIfAbsent(topic, key -> new ConcurrentLinkedQueue<>()).offer(messageListener);
  }

  @Override
  public void shutdown() {
    scheduledExecutor.shutdown();
  }

  private void consumeSilently(String message, Consumer<String> listener) {
    try {
      listener.accept(message);
    } catch (Exception e) {
      log.warn("Unexpected exception during message handling", e);
    }
  }
}
