package com.mindarray;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;

import static com.mindarray.Constants.*;

public class DiscoveryEngine extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise)  {

        vertx.eventBus().<JsonObject>consumer(RUN_DISCOVERY,handler->{

            JsonObject userData = handler.body();

            userData.put(CATEGORY,"discovery");

            vertx.executeBlocking(blockingHandler -> {

                JsonObject result = Utils.ping(userData.getString(IP_ADDRESS));

                if(result.getString(STATUS).equalsIgnoreCase(SUCCESS)){

                    JsonObject outcome = Utils.plugin(userData);

                    if(outcome.getString(STATUS).equalsIgnoreCase(SUCCESS)){

                        blockingHandler.complete(outcome);

                    }else{

                        blockingHandler.complete(outcome);

                    }

                }else{

                    blockingHandler.complete(new JsonObject().put(STATUS,FAIL).put(MESSAGE,PING_FAIL));

                }

            }).onComplete(completionHandler ->{

                if(completionHandler.succeeded()){

                    handler.reply(completionHandler.result());

                }else{

                    handler.fail(-1,FAIL);

                }

            });

        });

        startPromise.complete();

    }
}
