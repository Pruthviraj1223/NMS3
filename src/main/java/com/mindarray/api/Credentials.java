package com.mindarray.api;

import com.mindarray.Bootstrap;

import com.mindarray.Constants;

import io.vertx.core.Vertx;

import io.vertx.core.http.HttpMethod;

import io.vertx.core.json.JsonArray;

import io.vertx.core.json.JsonObject;

import io.vertx.ext.web.Router;

import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;

public class Credentials {

    Vertx vertx = Bootstrap.vertx;

    public void init(Router credentialRouter) {

        credentialRouter.post("/credentials").handler(this::validate).handler(this::create);

        credentialRouter.get("/credentials").handler(this::get);

        credentialRouter.get("/credentials/:id").handler(this::validate).handler(this::getId);

        credentialRouter.put("/credentials").handler(this::validate).handler(this::update);

        credentialRouter.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST || routingContext.request().method() == HttpMethod.PUT) {

                JsonObject userData = routingContext.getBodyAsJson();

                if (userData != null) {

                    HashMap<String, Object> result;

                    result = new HashMap<>(userData.getMap());

                    for (String key : result.keySet()) {

                        Object values = result.get(key);

                        if (values instanceof String) {

                            userData.put(key, values.toString().trim());

                        }

                    }


                    if (routingContext.request().method() == HttpMethod.POST) {

                        userData.put(Constants.METHOD, Constants.CREDENTIAL_POST_CHECK);

                        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                            if (handler.succeeded()) {

                                routingContext.setBody(handler.result().body().toBuffer());

                                routingContext.next();

                            } else {

                                routingContext.response()

                                        .setStatusCode(400)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                            }

                        });

                    } else {

                        userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                        userData.put(Constants.TABLE_NAME,Constants.CREDENTIAL_TABLE);

                        userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

                        userData.put(Constants.TABLE_ID,userData.getString(Constants.CREDENTIAL_ID));

                        vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                            if (handler.succeeded()) {

                                routingContext.setBody(userData.toBuffer());

                                routingContext.next();

                            } else {

                                routingContext.response()

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                            }

                        });

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.MISSING_DATA).encodePrettily());

                }
            } else if (routingContext.request().method() == HttpMethod.GET) {

                JsonObject userData = new JsonObject();

                userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                userData.put(Constants.TABLE_NAME,Constants.CREDENTIAL_TABLE);

                userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

                userData.put(Constants.TABLE_ID,routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                    if (handler.succeeded()) {

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                    }

                });


            } else if (routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject userData = new JsonObject();

                userData.put(Constants.METHOD, Constants.CREDENTIAL_DELETE_NAME_CHECK);

                userData.put("id", routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                    if (handler.succeeded()) {

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                    }

                });


            }

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(400)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR,exception.getMessage()).encodePrettily());
        }

    }

    void create(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_INSERT);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonObject result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, result.getInteger(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    void get(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

        userData.put(Constants.TABLE_ID,"getall");

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    if(!result.isEmpty()){

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(result.encodePrettily());

                    }else{
                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).put(Constants.MESSAGE,Constants.NOT_PRESENT).encodePrettily());

                    }



                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    void getId(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

        userData.put(Constants.TABLE_ID,routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, result).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void delete(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_DELETE);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

        userData.put(Constants.TABLE_ID,routingContext.pathParam("id"));

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_UPDATE);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN,Constants.CREDENTIAL_ID);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {
                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

}
