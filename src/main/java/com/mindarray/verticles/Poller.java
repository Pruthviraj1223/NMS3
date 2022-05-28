package com.mindarray.verticles;

import com.mindarray.Constants;

import com.mindarray.Utils;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class Poller extends AbstractVerticle {

    public static final Logger LOG = LoggerFactory.getLogger(Poller.class.getName());

    ConcurrentHashMap<Integer,String> statusCheck = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise)  {

        vertx.eventBus().<JsonObject>localConsumer(Constants.EVENTBUS_POLLER, handler -> {

            JsonObject data = handler.body();

            data.put(Constants.CATEGORY,Constants.POLLING);

            vertx.<JsonObject>executeBlocking(blockingHandler -> {

                if(data.getString(Constants.METRIC_GROUP).equalsIgnoreCase("ping")){

                    JsonObject result = Utils.checkAvailibility(data.getString(Constants.IP_ADDRESS));

                    statusCheck.put(data.getInteger(Constants.MONITOR_ID),result.getString(Constants.STATUS));

                    if(result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)){

                        blockingHandler.complete(result);

                    }else{

                        blockingHandler.fail(Constants.PING_FAIL);

                    }

                }else{

                    if(statusCheck.containsKey(data.getInteger(Constants.MONITOR_ID))){

                        if(statusCheck.get(data.getInteger(Constants.MONITOR_ID)).equalsIgnoreCase(Constants.SUCCESS)){

                            JsonObject result = Utils.spawnProcess(data);

                            if (!result.containsKey(Constants.ERROR)) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(result.getString(Constants.ERROR));

                            }

                        }else{

                            blockingHandler.fail(Constants.PING_FAIL);

                        }

                    }else {

                        JsonObject result = Utils.spawnProcess(data);

                        if (!result.containsKey(Constants.ERROR)) {

                            blockingHandler.complete(result);

                        } else {

                            blockingHandler.fail(result.getString(Constants.ERROR));

                        }
                    }

                }

            }).onComplete(completionHandler -> {

                if(completionHandler.succeeded()){

                    JsonObject pollData = new JsonObject();

                    pollData.put(Constants.MONITOR_ID,data.getInteger(Constants.MONITOR_ID));

                    pollData.put(Constants.METRIC_GROUP,data.getString(Constants.METRIC_GROUP));

                    pollData.put(Constants.RESULT,completionHandler.result());

                    pollData.put("timestamp",data.getString("timestamp"));

                    pollData.put(Constants.METHOD,Constants.DATABASE_INSERT);

                    pollData.put(Constants.TABLE_NAME,Constants.POLLER);

                    LOG.error("Metric id = {} {} {}", data.getString(Constants.METRIC_ID), data.getString(Constants.IP_ADDRESS), completionHandler.result());

                    vertx.eventBus().send(Constants.EVENTBUS_DATABASE,pollData);

                }else{

                   LOG.error("Error : {} ",completionHandler.cause().getMessage());

                }

            });

        });

        startPromise.complete();
    }
}
