package com.syswin.library.messaging.redis.spring;

import com.syswin.library.messaging.MessageDeliverException;
import com.syswin.library.messaging.MqProducer;

public class RedisMqProducer implements MqProducer {

  private final MessageRedisTemplate redisTemplate;

  public RedisMqProducer(MessageRedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  @Override
  public void start() {

  }

  @Override
  public void send(String message, String topic, String tags, String keys) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void sendOrderly(String message, String topic) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void sendRandomly(String message, String topic) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void shutdown() {

  }

  private void doSend(String message, String topic) throws MessageDeliverException {
    try {
      redisTemplate.convertAndSend(topic, message);
    } catch (Exception e) {
      throw new MessageDeliverException(
          String.format("Failed to send message with topic: %s to Redis", topic),
          e);
    }
  }
}
