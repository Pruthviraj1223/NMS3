package com.mindarray.verticles;

import com.mindarray.Constants;

import com.mindarray.Utils;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CheckedOutputStream;

public class Poller extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(Poller.class.getName());

    private final ConcurrentHashMap<Integer,String> statusCheck = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise)  {

        vertx.eventBus().<JsonObject>localConsumer(Constants.EVENTBUS_POLLER, handler -> {

            vertx.<JsonObject>executeBlocking(blockingHandler -> {

                try {

                    if(handler.body()!=null) {

                        JsonObject data = handler.body();

                        data.put(Constants.CATEGORY, Constants.POLLING);

                        if (data.containsKey(Constants.METRIC_GROUP) && data.containsKey(Constants.METRIC_ID) && data.getString(Constants.METRIC_GROUP).equalsIgnoreCase("ping")) {

                            JsonObject result = Utils.checkAvailability(data.getString(Constants.IP_ADDRESS));

                            statusCheck.put(data.getInteger(Constants.MONITOR_ID), result.getString(Constants.STATUS));

                            if(!result.containsKey(Constants.ERROR)) {

                                if (result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)) {

                                    data.put(Constants.RESULT,result);

                                    blockingHandler.complete(data);

                                } else {

                                    blockingHandler.fail(Constants.PING_FAIL);

                                }

                            }else {

                                blockingHandler.fail(result.getString(Constants.ERROR));

                            }

                        } else {

                            if (statusCheck.containsKey(data.getInteger(Constants.MONITOR_ID))) {

                                if (statusCheck.get(data.getInteger(Constants.MONITOR_ID)).equalsIgnoreCase(Constants.SUCCESS)) {

                                    JsonObject result = Utils.spawnProcess(data);

                                    if (!result.containsKey(Constants.ERROR)) {

                                        data.put(Constants.RESULT,result);

                                        blockingHandler.complete(data);

                                    } else {

                                        blockingHandler.fail(result.getString(Constants.ERROR));

                                    }

                                } else {

                                    blockingHandler.fail(Constants.PING_FAIL);

                                }

                            } else {

                                JsonObject result = Utils.spawnProcess(data);

                                if (!result.containsKey(Constants.ERROR)) {

                                    data.put(Constants.RESULT,result);

                                    blockingHandler.complete(data);

                                } else {

                                    blockingHandler.fail(result.getString(Constants.ERROR));

                                }

                            }

                        }

                    } else {

                        blockingHandler.fail(Constants.FAIL);

                    }

                }catch (Exception exception){

                    LOG.debug("Error in Polling {}",exception.getMessage());

                    blockingHandler.fail(exception.getMessage());

                }

            }).onComplete(completionHandler -> {

                if(completionHandler.succeeded()){

                    JsonObject pollData = new JsonObject();

                    JsonObject data = completionHandler.result();

                    pollData.put(Constants.MONITOR_ID,data.getInteger(Constants.MONITOR_ID));

                    pollData.put(Constants.METRIC_GROUP,data.getString(Constants.METRIC_GROUP));

                    pollData.put(Constants.RESULT,data.getString(Constants.RESULT));

                    pollData.put("timestamp",data.getString("timestamp"));

                    pollData.put(Constants.METHOD,Constants.DATABASE_INSERT);

                    pollData.put(Constants.TABLE_NAME,Constants.POLLER);

                    LOG.error("Metric id = {} {} {}", data.getString(Constants.METRIC_ID), data.getString(Constants.IP_ADDRESS), data.getString(Constants.RESULT));

                    vertx.eventBus().send(Constants.EVENTBUS_DATABASE,pollData);

                }else{

                   LOG.error("Fail data :: {} ",completionHandler.cause().getMessage());

                }

            });

        });

        startPromise.complete();
    }
}
