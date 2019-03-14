package com.syswin.library.messaging;

import java.io.UnsupportedEncodingException;

public interface MqProducer {

  void start() throws MessagingException;

  void send(String message, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, MessagingException;

  void sendOrderly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException;

  void sendRandomly(String message, String topic) throws InterruptedException, UnsupportedEncodingException, MessagingException;

  void shutdown();
}
