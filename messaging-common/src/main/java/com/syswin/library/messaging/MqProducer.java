/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
   * @throws MessageClientException when unexpected error occurred with MQ client
   * @throws MessageDeliverException when unexpected error occurred with connection between MQ client and broker
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
   * @throws MessageClientException when unexpected error occurred with MQ client
   * @throws MessageDeliverException when unexpected error occurred with connection between MQ client and broker
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
   * @throws MessageClientException when unexpected error occurred with MQ client
   * @throws MessageDeliverException when unexpected error occurred with connection between MQ client and broker
   */
  void sendRandomly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException;

  void shutdown();
}
