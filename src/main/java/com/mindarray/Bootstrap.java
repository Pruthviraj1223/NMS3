package com.mindarray;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    public static Vertx vertx = Vertx.vertx();

    static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class.getName());


    public static void main(String[] args) {


        start(ApiRouter.class.getName())

                .compose(future -> start(DatabaseEngine.class.getName()))

                .onComplete(handler->{

                    if(handler.succeeded()){

                        LOG.debug("Deployed");

                    }else{

                        LOG.debug("Deployed Unsuccessfully");

                    }

                });
        }

    public static Future<Void> start(String verticle) {

        Promise<Void> promise = Promise.promise();

        vertx.deployVerticle(verticle, handler->{

            if(handler.succeeded()){

                promise.complete();

            }else{

                promise.fail(handler.cause());

            }

        });

        return promise.future();

    }


}
