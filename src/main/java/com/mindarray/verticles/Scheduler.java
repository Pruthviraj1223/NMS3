package com.mindarray.verticles;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;

import java.util.Map;

public class Scheduler extends AbstractVerticle {

    public static final Logger LOG = LoggerFactory.getLogger(Scheduler.class.getName());

    HashMap<Integer,Integer> duplicate = new HashMap<>();

    HashMap<Integer,Integer> original = new HashMap<>();

    @Override
    public void start(Promise<Void> startPromise)  {

        vertx.eventBus().<JsonArray>localConsumer(Constants.SCHEDULER, handler -> {

            JsonArray metric = handler.body();

            for(int i=0;i<metric.size();i++)
            {
                var data = metric.getJsonObject(i);

                original.put(data.getInteger(Constants.METRIC_ID),data.getInteger(Constants.TIME));

                duplicate.put(data.getInteger(Constants.METRIC_ID),data.getInteger(Constants.TIME));

            }

        });

        vertx.setPeriodic(10000,handler -> {

            for(Map.Entry<Integer,Integer> entry: duplicate.entrySet()){

                int time = entry.getValue();

                time = time - 10000;

                if(time<=0){

                    vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE,new JsonObject().put(Constants.METHOD,Constants.INITIAL_POLLING).put(Constants.METRIC_ID,entry.getKey()), dbHandler -> {

                        if(dbHandler.succeeded()){

                            vertx.eventBus().send(Constants.EVENTBUS_POLLER,dbHandler.result().body());

                            duplicate.put(entry.getKey(),original.get(entry.getKey()));

                        }else{

                            LOG.debug("Error {}" ,dbHandler.cause().getMessage());

                        }

                    });

                }else{

                    duplicate.put(entry.getKey(),time);

                }

            }

        });

        startPromise.complete();

    }
}
