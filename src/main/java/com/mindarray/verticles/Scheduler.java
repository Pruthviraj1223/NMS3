package com.mindarray.verticles;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonArray;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;

import java.util.Map;

public class Scheduler extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise)  {

        HashMap<String,JsonObject> context = new HashMap<>();

        HashMap<String,Integer> original = new HashMap<>();

        vertx.eventBus().<JsonArray>localConsumer(Constants.SCHEDULER, handler -> {

            JsonArray metric = handler.body();

            for(int i=0;i<metric.size();i++){

                context.put(metric.getJsonObject(i).getString(Constants.METRIC_ID),metric.getJsonObject(i));

                original.put(metric.getJsonObject(i).getString(Constants.METRIC_ID),metric.getJsonObject(i).getInteger(Constants.TIME));

            }

        });

        vertx.setPeriodic(10000,handler -> {

            for(Map.Entry<String,Integer> entry: original.entrySet()){

                int time = entry.getValue();

                time = time - 10000;

                if(time<=0){

                    vertx.eventBus().send(Constants.EVENTBUS_POLLER,context.get(entry.getKey()));

                    JsonObject obj =  context.get(entry.getKey());

                    int oldTime = obj.getInteger(Constants.TIME);

                    original.put(entry.getKey(),oldTime);


                }else{

                    original.put(entry.getKey(),time);

                }

            }

        });

        startPromise.complete();

    }
}
