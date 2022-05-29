package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.mindarray.Constants.*;

public class Discovery {

    private final Vertx vertx = Bootstrap.vertx;

    private final Set<String> checkFields = new HashSet<>(Arrays.asList(CREDENTIAL_ID, DISCOVERY_NAME, IP_ADDRESS, TYPE, PORT));

    public void init(Router discoveryRouter) {

        discoveryRouter.post("/discovery").handler(this::fieldValidate).handler(this::validate).handler(this::create);

        discoveryRouter.get("/discovery").handler(this::get);

        discoveryRouter.get("/discovery/:id").handler(this::validate).handler(this::getId);

        discoveryRouter.put("/discovery").handler(this::fieldValidate).handler(this::validate).handler(this::update);

        discoveryRouter.delete("/discovery/:id").handler(this::validate).handler(this::delete);

        discoveryRouter.post("/discovery/run/:id").handler(this::validate).handler(this::merge).handler(this::runDiscovery);

    }

    private void fieldValidate(RoutingContext routingContext) {

        try {

            JsonObject user = routingContext.getBodyAsJson();

            if (user != null) {

                Set<String> fieldNames = user.fieldNames();

                if (routingContext.request().method() == HttpMethod.POST) {

                    if (fieldNames.size() >= checkFields.size()) {

                        JsonObject newUserData = new JsonObject();

                        for (String field : fieldNames) {

                            if (checkFields.contains(field)) {

                                newUserData.put(field, user.getValue(field));

                            }

                        }

                        routingContext.setBody(newUserData.toBuffer());

                        routingContext.next();


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());


                    }


                } else if (routingContext.request().method() == HttpMethod.PUT) {

                    if (user.containsKey(DISCOVERY_TABLE_ID)) {

                        JsonObject newUserData = new JsonObject();

                        for (String field : fieldNames) {

                            if (checkFields.contains(field)) {

                                newUserData.put(field, user.getValue(field));

                            }

                        }

                        newUserData.put(DISCOVERY_TABLE_ID,user.getValue(DISCOVERY_TABLE_ID));

                        routingContext.setBody(newUserData.toBuffer());

                        routingContext.next();


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, DISCOVERY_TABLE_ID + " is missing").encodePrettily());


                    }


                }


            } else {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());


            }
        } catch (Exception exception) {


            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());

        }


    }

    private void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST || routingContext.request().method() == HttpMethod.PUT) {

                JsonObject userData = routingContext.getBodyAsJson();

                if (userData != null && !routingContext.request().params().isEmpty()) {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());


                } else if (!routingContext.request().params().isEmpty()) {

                    JsonObject data = new JsonObject();

                    data.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                    data.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                    data.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                    data.put(Constants.TABLE_ID, routingContext.pathParam("id"));

                    vertx.eventBus().request(Constants.EVENTBUS_DATABASE, data, handler -> {

                        if (handler.succeeded()) {

                            routingContext.next();

                        } else {

                            routingContext.response()

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.NOT_PRESENT).encodePrettily());

                        }

                    });

                } else {

                    if (userData != null) {

                        HashMap<String, Object> result;

                        result = new HashMap<>(userData.getMap());

                        for (String key : result.keySet()) {

                            Object val = result.get(key);

                            if (val instanceof String) {

                                userData.put(key, val.toString().trim());

                            }

                        }

                        if (routingContext.request().method() == HttpMethod.POST) {

                            if ((userData.containsKey(Constants.CREDENTIAL_ID) && userData.containsKey(Constants.DISCOVERY_NAME) && userData.containsKey(Constants.PORT) && userData.containsKey(Constants.TYPE) && userData.containsKey(Constants.IP_ADDRESS))) {

                                userData.put(Constants.METHOD, DISCOVERY_POST_CHECK);

                                vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                                    if (handler.succeeded()) {

                                        JsonObject response = handler.result().body();

                                        routingContext.setBody(response.toBuffer());

                                        routingContext.next();

                                    } else {

                                        routingContext.response()

                                                .setStatusCode(400)

                                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                                    }

                                });

                            } else {

                                routingContext.response()

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());


                            }

                        } else {

                            userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                            userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                            userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                            userData.put(Constants.TABLE_ID, userData.getString(Constants.DISCOVERY_TABLE_ID));

                            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                                if (handler.succeeded()) {

                                    routingContext.setBody(userData.toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());

                                }

                            });

                        }


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());


                    }
                }

            } else if (routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject userData = new JsonObject();

                userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                    if (handler.succeeded()) {

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.NOT_PRESENT).encodePrettily());

                    }

                });

            } else if (routingContext.request().method() == HttpMethod.GET) {

                JsonObject userData = new JsonObject();

                userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

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

                    .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());
        }

    }

    private void create(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_INSERT);

        userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.DISCOVERY_TABLE_ID, response.result().body().getInteger(Constants.DISCOVERY_TABLE_ID)).encodePrettily());

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                    }

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

    private void get(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(METHOD, DATABASE_GET);

        userData.put(TABLE_NAME, DISCOVERY_TABLE);

        userData.put(TABLE_COLUMN, DISCOVERY_TABLE_ID);

        userData.put(TABLE_ID, GETALL);

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        JsonArray jsonArray = response.result().body();

                        if (!jsonArray.isEmpty()) {

                            routingContext.response()

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(jsonArray.encodePrettily());
                        } else {

                            routingContext.response()

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, Constants.NOT_PRESENT).encodePrettily());

                        }

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

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

    private void getId(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

        userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        JsonArray result = response.result().body();

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(result.encodePrettily());
                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    private void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_UPDATE);

        userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);


        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void delete(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_DELETE);

        userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

        userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void merge(RoutingContext routingContext) {

        try {

            JsonObject data = new JsonObject();

            data.put(Constants.METHOD, MERGE_DATA);

            data.put("id", routingContext.pathParam("id"));

            vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {

                if (handler.succeeded()) {

                    if (handler.result().body() != null) {

                        routingContext.setBody(handler.result().body().toBuffer());

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                    }

                }

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }
    }

    private void runDiscovery(RoutingContext routingContext) {

        try {

            JsonObject userData = routingContext.getBodyAsJson();

            vertx.eventBus().<JsonObject>request(RUN_DISCOVERY, userData, response -> {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        JsonObject data = new JsonObject();

                        data.put(METHOD, RUN_DISCOVERY_INSERT);

                        data.put("result", response.result().body());

                        data.put(DISCOVERY_TABLE_ID, userData.getString(DISCOVERY_TABLE_ID));

                        vertx.eventBus().request(EVENTBUS_DATABASE, data, handler -> {

                            if (handler.succeeded()) {

                                routingContext.response()

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(response.result().body().encodePrettily());

                            } else {

                                routingContext.response()

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                            }

                        });

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                    }


                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, response.cause().getMessage()).encodePrettily());

                }

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }
    }

}
