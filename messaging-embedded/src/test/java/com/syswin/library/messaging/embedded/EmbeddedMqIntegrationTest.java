package com.syswin.library.messaging.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.Test;

public class EmbeddedMqIntegrationTest {
  private static final String topic = "brave-new-world";
  private static final String tag = "*";
  private static final String keys = "";

  private static final String message1 = "message1";
  private static final String message2 = "message2";
  private static final String message3 = "message3";

  private final BlockingQueue<String> messages = new ArrayBlockingQueue<>(10);
  private final EmbeddedMessageQueue messageQueue = new EmbeddedMessageQueue();
  private final EmbeddedMqProducer mqProducer = new EmbeddedMqProducer(messageQueue);
  private final EmbeddedMqConsumer embeddedMqConsumer = new EmbeddedMqConsumer(messageQueue, topic);

  @Test
  public void sendMessage() {
    embeddedMqConsumer.start(messages::add);

    mqProducer.send(message1, topic, tag, keys);
    mqProducer.sendOrderly(message2, topic);
    mqProducer.sendRandomly(message3, topic);

    assertThat(messages).containsExactly(message1, message2, message3);
  }
}
