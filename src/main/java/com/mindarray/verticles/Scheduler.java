package com.mindarray.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Scheduler extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise)  {


        HashMap<String,Object> map = new HashMap<>();

        vertx.eventBus().<JsonObject>localConsumer("sch1", handler -> {

            System.out.println("data in schedulerr" + handler.body());

            map.put(handler.body().getString(Constants.IP_ADDRESS),handler.body());


        });

        vertx.setPeriodic(3000,handler -> {

            for(Map.Entry<String,Object> mp : map.entrySet()){

                 System.out.println("key " + mp.getKey());
                System.out.println("val  " + mp.getValue());


            }


        });


        startPromise.complete();
    }
}
