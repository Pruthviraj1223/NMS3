package com.mindarray.api;

import io.vertx.core.json.JsonObject;
import org.zeromq.ZMQ;

import java.util.Base64;
import java.util.Scanner;

public class GoDatabase {

    public static ZMQ.Socket Init() {

        ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket subscriber = context.socket(ZMQ.PUSH);

        subscriber.connect("tcp://10.20.40.224:4567");

        return subscriber;

    }

    public void sendToDB(JsonObject data,ZMQ.Socket subscriber) {

        subscriber.send(Base64.getEncoder().encodeToString(data.toString().getBytes()));

    }

}
