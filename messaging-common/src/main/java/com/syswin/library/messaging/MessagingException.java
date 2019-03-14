package com.syswin.library.messaging;

public class MessagingException extends Exception {

  public MessagingException(String message) {
    super(message);
  }

  public MessagingException(String message, Throwable e) {
    super(message, e);
  }
}
