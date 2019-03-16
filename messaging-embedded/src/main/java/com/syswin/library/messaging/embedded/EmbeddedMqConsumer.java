package com.syswin.library.messaging.embedded;

import com.syswin.library.messaging.MqConsumer;
import java.util.function.Consumer;

public class EmbeddedMqConsumer implements MqConsumer {

  private final String topic;
  private final MessageQueue messageQueue;
  private final Consumer<String> messageListener;

  public EmbeddedMqConsumer(MessageQueue messageQueue, String topic, Consumer<String> messageListener) {
    this.topic = topic;
    this.messageQueue = messageQueue;
    this.messageListener = messageListener;
  }

  @Override
  public void start() {
    messageQueue.subscribe(topic, messageListener);
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String topic() {
    return topic;
  }
}
