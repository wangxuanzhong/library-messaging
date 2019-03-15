package com.syswin.library.messaging.embedded;

import com.syswin.library.messaging.MqProducer;

public class EmbeddedMqProducer implements MqProducer {

  private final MessageQueue messageQueue;

  public EmbeddedMqProducer(MessageQueue messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public void start() {

  }

  @Override
  public void send(String message, String topic, String tags, String keys) {
    messageQueue.send(topic, message);
  }

  @Override
  public void sendOrderly(String message, String topic) {
    messageQueue.send(topic, message);
  }

  @Override
  public void sendRandomly(String message, String topic) {
    messageQueue.send(topic, message);
  }

  @Override
  public void shutdown() {

  }
}
