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

package com.syswin.library.messaging.rocketmq;

import com.syswin.library.messaging.MessageBrokerException;
import com.syswin.library.messaging.MessageClientException;
import com.syswin.library.messaging.MessageDeliverException;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqProducer implements MqProducer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final DefaultMQProducer producer;
  private final String brokerAddress;

  public RocketMqProducer(String brokerAddress, String producerGroup) {
    this.brokerAddress = brokerAddress;
    this.producer = new DefaultMQProducer(producerGroup);
  }

  @Override
  public final void start() throws MessagingException {
    try {
      producer.setNamesrvAddr(brokerAddress);
      producer.setInstanceName(UUID.randomUUID().toString());
      producer.start();
      log.info("Rocket MQ producer {} in group {} started successfully with broker {}",
          producer.getInstanceName(),
          producer.getProducerGroup(),
          brokerAddress);
    } catch (MQClientException e) {
      throw new MessagingException("Failed to start Rocket MQ producer with broker " + brokerAddress, e);
    }
  }

  @Override
  public final void send(String message, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, MessagingException {
    doSend(message, topic, tags, keys, this::loadBalancedSend);
  }

  @Override
  public final void sendOrderly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException {
    doSend(message, topic, "", "", this::loadBalancedSend);
  }

  @Override
  public final void sendRandomly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException {
    doSend(message, topic, "", "", producer::send);
  }

  @Override
  public final void shutdown() {
    producer.shutdown();
    log.info("Rocket MQ producer {} in group {} shut down successfully", producer.getInstanceName(), producer.getProducerGroup());
  }

  private void doSend(String message,
      String topic,
      String tags,
      String keys,
      MsgSender<Message, SendResult> msgSender)
      throws UnsupportedEncodingException, InterruptedException, MessagingException {

    Message mqMsg = new Message(topic, tags, keys, message.getBytes(RemotingHelper.DEFAULT_CHARSET));
    log.debug("Sending message {} with topic: {}, tag: {}, keys: {} to Rocket MQ", message, topic, tags, keys);
    try {
      SendResult sendResult = msgSender.send(mqMsg);
      log.debug("Sent message {} with topic: {}, tag: {}, keys: {} to Rocket MQ with result {}", message, topic, tags, keys, sendResult);
      if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
        throw new MessageBrokerException(
            String.format("Failed to send message with topic: %s, tag: %s, keys: %s to Rocket MQ %s with result: %s",
                topic,
                tags,
                keys,
                brokerAddress,
                sendResult));
      }
    } catch (MQClientException e) {
      throw new MessageClientException(description(topic, tags, keys), e);
    } catch (RemotingException | MQBrokerException e) {
      throw new MessageDeliverException(description(topic, tags, keys), e);
    }
  }

  private String description(String topic, String tags, String keys) {
    return String.format("Failed to send message with topic: %s, tag: %s, keys: %s to Rocket MQ %s", topic, tags, keys, brokerAddress);
  }

  private SendResult loadBalancedSend(Message message)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    String shardKey = message.getTags() == null ? message.getTopic() : message.getTags();
    return producer.send(message, (queues, msg, arg) -> {
      int index = Math.abs(arg.hashCode() % queues.size());
      return queues.get(index);
    }, shardKey);
  }
}
