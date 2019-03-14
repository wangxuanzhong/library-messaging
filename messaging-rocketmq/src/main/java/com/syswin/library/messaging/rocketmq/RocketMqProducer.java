package com.syswin.library.messaging.rocketmq;

import com.syswin.library.messaging.MessageBrokerException;
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
    } catch (MQClientException | RemotingException | MQBrokerException e) {
      throw new MessageDeliverException(
          String.format("Failed to send message with topic: %s, tag: %s, keys: %s to Rocket MQ %s", topic, tags, keys, brokerAddress),
          e);
    }
  }

  private SendResult loadBalancedSend(Message message)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    String shardKey = message.getTags() == null ? message.getTopic() : message.getTags();
    return producer.send(message, (queues, msg, arg) -> {
      int hash = Math.abs(arg.hashCode());
      int index = hash % queues.size();
      return queues.get(index);
    }, shardKey);
  }
}
