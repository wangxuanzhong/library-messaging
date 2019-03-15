package com.syswin.library.messaging.embedded;

import com.syswin.library.messaging.MqConsumer;
import java.util.function.Consumer;

public class EmbeddedMqConsumer implements MqConsumer {

  private final String topic;
  private final MessageQueue messageQueue;

  public EmbeddedMqConsumer(MessageQueue messageQueue, String topic) {
    this.topic = topic;
    this.messageQueue = messageQueue;
  }

  @Override
  public void start(Consumer<String> messageListener) {
    messageQueue.subscribe(topic, messageListener);
  }

  @Override
  public void shutdown() {

  }
}
