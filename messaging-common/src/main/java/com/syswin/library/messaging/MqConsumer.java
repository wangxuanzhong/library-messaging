package com.syswin.library.messaging;

import java.util.function.Consumer;

public interface MqConsumer {

  void start(Consumer<String> messageListener) throws MessagingException;

  void shutdown();
}
