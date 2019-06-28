package com.syswin.library.messaging.rocketmqons;


import com.syswin.library.messaging.MessagingException;

interface MsgSender<R, T> {

  T send(R message) throws MessagingException;
}
