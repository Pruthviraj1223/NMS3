package com.mindarray;

import com.mindarray.verticles.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class.getName());

    public static Vertx vertx = Vertx.vertx();

    public static void main(String[] args) {

        start(ApiRouter.class.getName())

                .compose(future -> start(DatabaseEngine.class.getName()))

                .compose(future -> start(DiscoveryEngine.class.getName()))

                .compose(future -> start(MetricScheduler.class.getName()))

                .compose(future -> start(Poller.class.getName()))

                .onComplete(handler -> {

                    if (handler.succeeded()) {

                        LOG.debug("Deployed Successfully");

                    } else {

                        LOG.debug("Deployed Unsuccessfully");

                    }

                });

    }

    public static Future<Void> start(String verticle) {

        Promise<Void> promise = Promise.promise();

        vertx.deployVerticle(verticle, handler -> {

            if (handler.succeeded()) {

                promise.complete();

            } else {

                promise.fail(handler.cause());

            }

        });

        return promise.future();

    }


}
