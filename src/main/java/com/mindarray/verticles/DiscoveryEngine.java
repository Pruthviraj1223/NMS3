package com.mindarray.verticles;

import com.mindarray.Utils;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import static com.mindarray.Constants.*;

public class DiscoveryEngine extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        vertx.eventBus().<JsonObject>localConsumer(RUN_DISCOVERY, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                try {

                    if (handler.body() != null) {

                        JsonObject userData = handler.body();

                        userData.put(CATEGORY, "discovery");

                        JsonObject result = Utils.checkAvailability(userData.getString(IP_ADDRESS));

                        if (!result.containsKey(ERROR)) {

                            if (result.getString(STATUS).equalsIgnoreCase(SUCCESS)) {

                                JsonObject outcome = Utils.spawnProcess(userData);

                                if (!outcome.containsKey(ERROR)) {

                                    if (outcome.getString(STATUS).equalsIgnoreCase(SUCCESS)) {

                                        blockingHandler.complete(outcome);

                                    } else {

                                        blockingHandler.complete(outcome);

                                    }

                                } else {

                                    blockingHandler.fail(outcome.getString(ERROR));

                                }

                            } else {

                                blockingHandler.fail(PING_FAIL);

                            }

                        } else {

                            blockingHandler.fail(result.getString(ERROR));

                        }

                    } else {

                        blockingHandler.fail(FAIL);

                    }

                } catch (Exception exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    blockingHandler.fail(exception.getMessage());

                }

            }).onComplete(completionHandler -> {

                if (completionHandler.succeeded()) {

                    handler.reply(completionHandler.result());

                } else {

                    handler.fail(-1, completionHandler.cause().getMessage());

                }

            });

        });

        startPromise.complete();

    }
}
