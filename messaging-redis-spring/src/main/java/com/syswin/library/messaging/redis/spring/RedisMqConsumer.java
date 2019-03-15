package com.syswin.library.messaging.redis.spring;

import com.syswin.library.messaging.MqConsumer;
import java.util.function.Consumer;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

public class RedisMqConsumer implements MqConsumer, MessageListener {

  private final Consumer<String> messageListener;
  private final String topic;

  public RedisMqConsumer(String topic, Consumer<String> messageListener) {
    this.messageListener = messageListener;
    this.topic = topic;
  }

  @Override
  public void start() {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void onMessage(Message message, byte[] pattern) {
    messageListener.accept(message.toString());
  }

  String topic() {
    return topic;
  }
}
