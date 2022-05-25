package com.mindarray.verticles;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class Poller extends AbstractVerticle {

    public static final Logger LOG = LoggerFactory.getLogger(Poller.class.getName());

    ConcurrentHashMap<Integer,String> check = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise)  {

        vertx.eventBus().<JsonObject>consumer(Constants.EVENTBUS_POLLER, handler -> {

            JsonObject data = handler.body();

            vertx.executeBlocking(blockingHandler -> {

//                LOG.debug("Thread {}",Thread.currentThread().getName());

                data.put(Constants.CATEGORY,"polling");

                if(data.getString(Constants.METRIC_GROUP).equalsIgnoreCase("ping")){

                    JsonObject result = Utils.ping(data.getString(Constants.IP_ADDRESS));

                    check.put(data.getInteger(Constants.MONITOR_ID),result.getString(Constants.STATUS));

                    if(result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)){

                        blockingHandler.complete(result);

                    }else{

                        blockingHandler.fail(Constants.PING_FAIL);

                    }


                }else{

                    if(check.containsKey(data.getInteger(Constants.MONITOR_ID))){

                        if(check.get(data.getInteger(Constants.MONITOR_ID)).equalsIgnoreCase(Constants.SUCCESS)){

                            JsonObject result = Utils.plugin(data);

                            if (result != null) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(Constants.FAIL);

                            }

                        }else{

                            blockingHandler.fail(Constants.PING_FAIL);

                        }

                    }else {

                        JsonObject result = Utils.plugin(data);

                        if (result != null) {

                            blockingHandler.complete(result);

                        } else {

                            blockingHandler.fail(Constants.FAIL);

                        }
                    }

                }

            }).onComplete(completionHandler -> {

                if(completionHandler.succeeded()){

                    System.out.println("Metric id = " + data.getString(Constants.METRIC_ID) + " ip  = " + data.getString(Constants.IP_ADDRESS) + " polling data is  " + completionHandler.result());

                }else{

                   LOG.debug("Error {}",completionHandler.cause().getMessage());

                }

            });

        });



        startPromise.complete();
    }
}
