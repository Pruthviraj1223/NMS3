package com.mindarray.verticles;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonArray;

import java.util.HashMap;
import java.util.Map;

public class Scheduler extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise)  {

        HashMap<String,Object> context = new HashMap<>();

        HashMap<String,Integer> original = new HashMap<String, Integer>();

        vertx.eventBus().<JsonArray>localConsumer(Constants.SCHEDULER, handler -> {

            JsonArray metric = handler.body();

            for(int i=0;i<metric.size();i++){

                context.put(metric.getJsonObject(i).getString(Constants.METRIC_ID),metric.getJsonObject(i));

                original.put(metric.getJsonObject(i).getString(Constants.METRIC_ID),metric.getJsonObject(i).getInteger(Constants.TIME));

            }

        });

        vertx.setPeriodic(5000,handler -> {

            for(Map.Entry<String,Integer> entry: original.entrySet()){

                int time = entry.getValue();

                if(time<=0){



                }else{

                    time = time - 10000;

                    original.put(entry.getKey(),time);

                }


            }


        });


        startPromise.complete();
    }
}
