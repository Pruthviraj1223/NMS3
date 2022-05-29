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

public class Credentials {

    private final Vertx vertx = Bootstrap.vertx;

    private final Set<String> checkFields = new HashSet<>(Arrays.asList(CREDENTIAL_NAME, PROTOCOL, NAME, PASSWORD, COMMUNITY, VERSION));

    private final Set<String> checkFields1 = new HashSet<>(Arrays.asList(CREDENTIAL_NAME, PROTOCOL, NAME, PASSWORD));

    private final Set<String> checkFields2 = new HashSet<>(Arrays.asList(CREDENTIAL_NAME, PROTOCOL, COMMUNITY, VERSION));

    public void init(Router credentialRouter) {

        credentialRouter.post("/credentials").handler(this::fieldValidate).handler(this::validate).handler(this::create);

        credentialRouter.get("/credentials").handler(this::get);

        credentialRouter.get("/credentials/:id").handler(this::validate).handler(this::getId);

        credentialRouter.put("/credentials").handler(this::fieldValidate).handler(this::validate).handler(this::update);

        credentialRouter.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    private void fieldValidate(RoutingContext routingContext) {

        try {

            JsonObject user = routingContext.getBodyAsJson();

            if (user != null) {

                Set<String> fieldNames = user.fieldNames();

                if (routingContext.request().method() == HttpMethod.POST) {

                    if (user.containsKey(PROTOCOL) && (user.getString(PROTOCOL).equalsIgnoreCase(SSH) || user.getString(PROTOCOL).equalsIgnoreCase(WINRM))) {

                        if (fieldNames.size() >= checkFields1.size()) {

                            JsonObject newUserData = new JsonObject();

                            for (String field : fieldNames) {

                                if (checkFields1.contains(field)) {

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

                    } else if (user.containsKey(PROTOCOL) && user.getString(PROTOCOL).equalsIgnoreCase(SNMP)) {

                        if (fieldNames.size() >= checkFields2.size()) {

                            JsonObject newUserData = new JsonObject();

                            for (String field : fieldNames) {

                                if (checkFields2.contains(field)) {

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


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, "Invalid Protocol").encodePrettily());

                    }


                } else if (routingContext.request().method() == HttpMethod.PUT) {

                    if (user.containsKey(CREDENTIAL_ID)) {

                        JsonObject newUserData = new JsonObject();

                        for (String field : fieldNames) {

                            if (checkFields.contains(field)) {

                                newUserData.put(field, user.getValue(field));

                            }

                        }

                        newUserData.put(CREDENTIAL_ID, user.getValue(CREDENTIAL_ID));

                        routingContext.setBody(newUserData.toBuffer());

                        routingContext.next();


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, CREDENTIAL_ID + " is missing").encodePrettily());


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

                            try {

                                if (handler.succeeded()) {

                                    routingContext.setBody(handler.result().body().toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                                }

                            } catch (Exception exception) {

                                System.out.println(exception.getMessage());

                            }

                        });


                    } else {

                        userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

                        userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

                        userData.put(Constants.TABLE_ID, userData.getString(Constants.CREDENTIAL_ID));

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

                userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

                userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

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

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, exception.getMessage()).encodePrettily());
        }

    }

    private void create(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_INSERT);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

//                    JsonObject result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, response.result().body().getInteger(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void get(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        userData.put(Constants.TABLE_ID, Constants.GETALL);

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    if (!result.isEmpty()) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(result.encodePrettily());

                    } else {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, Constants.NOT_PRESENT).encodePrettily());

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

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

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

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

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

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

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

        userData.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        userData.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

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
