package com.syswin.library.messaging.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

interface MsgSender<R, T> {

  T send(R message) throws MQClientException, RemotingException, MQBrokerException, InterruptedException ;
}
