package com.mindarray.verticles;

import com.mindarray.Constants;
import com.mindarray.Utils;
import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import jdk.swing.interop.SwingInterOpUtils;
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

                data.put(Constants.CATEGORY,Constants.POLLING);

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

                            if (!result.containsKey(Constants.ERROR)) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(result.getString(Constants.ERROR));

                            }

                        }else{

                            blockingHandler.fail(Constants.PING_FAIL);

                        }

                    }else {

                        JsonObject result = Utils.plugin(data);

                        if (!result.containsKey(Constants.ERROR)) {

                            blockingHandler.complete(result);

                        } else {

                            blockingHandler.fail(result.getString(Constants.ERROR));

                        }
                    }

                }

            }).onComplete(completionHandler -> {

                if(completionHandler.succeeded()){

                    JsonObject poll = new JsonObject();

                    poll.put(Constants.MONITOR_ID,data.getInteger(Constants.MONITOR_ID));

                    poll.put(Constants.METRIC_GROUP,data.getString(Constants.METRIC_GROUP));

                    poll.put(Constants.RESULT,completionHandler.result());

                    poll.put("timestamp",data.getString("timestamp"));

                    poll.put(Constants.METHOD,Constants.DATABASE_INSERT);

                    poll.put(Constants.TABLE_NAME,Constants.POLLER);

                    vertx.eventBus().send(Constants.EVENTBUS_DATABASE,poll);

                    LOG.info("Metric id = {} {} {}", data.getString(Constants.METRIC_ID), data.getString(Constants.IP_ADDRESS), completionHandler.result());

                }else{

                   LOG.debug("Error {} ",completionHandler.cause().getMessage());

                }

            });

        });

        startPromise.complete();
    }
}
