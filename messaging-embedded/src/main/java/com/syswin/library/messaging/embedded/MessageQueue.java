package com.syswin.library.messaging.embedded;

import java.util.function.Consumer;

public interface MessageQueue {

  void send(String topic, String message);

  void subscribe(String topic, Consumer<String> messageListener);
}
