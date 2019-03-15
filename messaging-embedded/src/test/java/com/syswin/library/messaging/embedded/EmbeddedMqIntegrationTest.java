package com.syswin.library.messaging.embedded;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmbeddedMqIntegrationTest {
  private static final String topic1 = "brave-new-world";
  private static final String topic2 = "rise-and-fall";
  private static final String tag = "*";
  private static final String keys = "";

  private static final String message1 = "message1";
  private static final String message2 = "message2";
  private static final String message3 = "message3";

  private final BlockingQueue<String> messages1 = new ArrayBlockingQueue<>(10);
  private final BlockingQueue<String> messages2 = new ArrayBlockingQueue<>(10);
  private final EmbeddedMessageQueue messageQueue = new EmbeddedMessageQueue();
  private final EmbeddedMqProducer mqProducer = new EmbeddedMqProducer(messageQueue);
  private final EmbeddedMqConsumer embeddedMqConsumer1 = new EmbeddedMqConsumer(messageQueue, topic1, messages1::add);
  private final EmbeddedMqConsumer embeddedMqConsumer2 = new EmbeddedMqConsumer(messageQueue, topic2, messages2::add);

  @Before
  public void setUp() {
    messageQueue.start();
  }

  @After
  public void tearDown() {
    messageQueue.shutdown();
  }

  @Test
  public void sendMessage() {
    embeddedMqConsumer1.start();
    embeddedMqConsumer2.start();

    mqProducer.send(message1, topic1, tag, keys);
    mqProducer.sendOrderly(message2, topic2);
    mqProducer.sendRandomly(message3, topic1);

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages1).hasSize(2));
    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages2).hasSize(1));
    assertThat(messages1).containsExactly(message1, message3);
    assertThat(messages2).containsExactly(message2);
  }

  @Test
  public void ignoreExceptionsByMessageListener() {
    EmbeddedMqConsumer embeddedMqConsumer = new EmbeddedMqConsumer(messageQueue, topic1, new ExceptionThrowingConsumer(messages1));
    embeddedMqConsumer.start();

    mqProducer.send(message2, topic1, tag, keys);
    mqProducer.send(message1, topic1, tag, keys);
    mqProducer.sendOrderly(message2, topic1);
    mqProducer.sendRandomly(message3, topic1);

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages1).hasSize(3));
    assertThat(messages1).containsExactly(message1, message2, message3);
  }

  private static class ExceptionThrowingConsumer implements Consumer<String> {

    private final Queue<String> messages;
    private final AtomicBoolean failed = new AtomicBoolean(true);

    ExceptionThrowingConsumer(Queue<String> messages) {
      this.messages = messages;
    }

    @Override
    public void accept(String message) {
      if (failed.compareAndSet(true, false)) {
        throw new IllegalStateException("oops");
      }
      messages.add(message);
    }
  }
}
