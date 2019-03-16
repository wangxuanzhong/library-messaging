package com.syswin.library.messaging;

import java.io.UnsupportedEncodingException;

public interface MqProducer {

  void start() throws MessagingException;

  /**
   * Send message with specified topic to MQ using hashcode of tags as shard key. Messages with the same tags will be sent via the same queue to keep
   * order of messages, provided the underlying MQ supports multiple queues per topic.
   *
   * @param message the message to be delivered
   * @param topic the topic to deliver the message in
   * @param tags the sharding key to allocate the message to a specific queue
   * @param keys the ID of the message
   * @throws UnsupportedEncodingException when failed to encode the message
   * @throws InterruptedException when interrupted when waiting to deliver the message
   * @throws MessageBrokerException when unexpected error occurred with MQ broker
   * @throws MessageDeliverException when unexpected error occurred with MQ client, or connection between MQ client and broker
   */
  void send(String message, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, MessagingException;

  /**
   * Send message with specified topic to MQ. Messages will be sent via the same queue to keep order of messages, provided the underlying MQ supports
   * multiple queues per topic.
   *
   * @param message the message to be delivered
   * @param topic the topic to deliver the message in
   * @throws UnsupportedEncodingException when failed to encode the message
   * @throws InterruptedException when interrupted when waiting to deliver the message
   * @throws MessageBrokerException when unexpected error occurred with MQ broker
   * @throws MessageDeliverException when unexpected error occurred with MQ client, or connection between MQ client and broker
   */
  void sendOrderly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException;

  /**
   * Send message with specified topic to MQ. Messages will be delivered evenly among queues of the specified topic, provided the underlying MQ
   * supports multiple queues per topic.
   *
   * @param message the message to be delivered
   * @param topic the topic to deliver the message in
   * @throws UnsupportedEncodingException when failed to encode the message
   * @throws InterruptedException when interrupted when waiting to deliver the message
   * @throws MessageBrokerException when unexpected error occurred with MQ broker
   * @throws MessageDeliverException when unexpected error occurred with MQ client, or connection between MQ client and broker
   */
  void sendRandomly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException;

  void shutdown();
}
