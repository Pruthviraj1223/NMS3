package com.mindarray;

import api.Credentials;
import api.Discovery;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;


public class ApiRouter extends AbstractVerticle {


    @Override
    public void start(Promise<Void> startPromise) {

        Router router = Router.router(vertx);

        Router discoveryRouter = Router.router(vertx);

        Router credentialRouter = Router.router(vertx);

        router.mountSubRouter("/api/", discoveryRouter);

        router.mountSubRouter("/api/", credentialRouter);

        router.route().handler(BodyHandler.create());

        discoveryRouter.route().handler(BodyHandler.create());

        credentialRouter.route().handler(BodyHandler.create());

        Credentials credentials = new Credentials();

        credentials.init(credentialRouter);


        vertx.createHttpServer().requestHandler(router).listen(8080);

        startPromise.complete();
    }
}
