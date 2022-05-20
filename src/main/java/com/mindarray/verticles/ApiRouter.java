package com.mindarray.verticles;

import com.mindarray.api.Credentials;

import com.mindarray.api.Discovery;

import com.mindarray.api.Monitor;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.ext.web.Router;

import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApiRouter extends AbstractVerticle {

    static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class.getName());

    @Override
    public void start(Promise<Void> startPromise)  {

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

        }catch (Exception exception){

            LOG.debug("Error {}",exception.getMessage());

        }

        Router router = Router.router(vertx);

        Router subrouter = Router.router(vertx);

        Router monitorRouter = Router.router(vertx);

        router.mountSubRouter("/api", subrouter);

        router.mountSubRouter("/api/monitor/",monitorRouter);

        router.route().handler(BodyHandler.create());

        subrouter.route().handler(BodyHandler.create());

        monitorRouter.route().handler(BodyHandler.create());

        Credentials credentials = new Credentials();

        credentials.init(subrouter);

        Discovery discovery = new Discovery();

        discovery.init(subrouter);

        Monitor monitor = new Monitor();

        monitor.init(monitorRouter);

        vertx.createHttpServer().requestHandler(router).listen(8080);

        startPromise.complete();

    }
}