package com.syswin.library.messaging;

public class MessageDeliverException extends MessagingException {

  public MessageDeliverException(String message, Throwable e) {
    super(message, e);
  }
}
