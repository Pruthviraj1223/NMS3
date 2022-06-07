package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.mindarray.Constants.*;

public class Credentials {

    private static final Logger LOG = LoggerFactory.getLogger(Credentials.class.getName());
    private final Vertx vertx = Bootstrap.vertx;
    private final Set<String> checkFields = Set.of(CREDENTIAL_NAME, PROTOCOL, USERNAME, PASSWORD);
    private final Set<String> validateFields = Set.of(CREDENTIAL_NAME, PROTOCOL, COMMUNITY, VERSION);

    public void init(Router credentialRouter) {

        credentialRouter.post("/credentials").handler(this::filter).handler(this::validate).handler(this::create);

        credentialRouter.get("/credentials").handler(this::get);

        credentialRouter.get("/credentials/:id").handler(this::validate).handler(this::getId);

        credentialRouter.put("/credentials/:id").handler(this::filter).handler(this::update);

        credentialRouter.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    private void filter(RoutingContext routingContext) {

        try {

            JsonObject user = routingContext.getBodyAsJson();

            if (user != null && !user.isEmpty()) {

                Set<String> userFields = user.fieldNames();

                if (routingContext.request().method() == HttpMethod.POST) {

                    if (user.containsKey(PROTOCOL) && (user.getString(PROTOCOL).equalsIgnoreCase(SSH) || user.getString(PROTOCOL).equalsIgnoreCase(WINRM))) {

                        if (userFields.size() >= checkFields.size()) {

                            Iterator<Map.Entry<String, Object>> iterator = user.iterator();

                            while (iterator.hasNext()) {

                                if (!checkFields.contains(iterator.next().getKey())) {

                                    iterator.remove();

                                }

                            }

                            for (Map.Entry<String, Object> entry : user) {

                                if (entry.getValue() instanceof String) {

                                    user.put(entry.getKey(), entry.getValue().toString().trim());

                                }
                            }

                            routingContext.setBody(user.toBuffer());

                            routingContext.next();


                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());

                        }

                    } else if (user.containsKey(PROTOCOL) && user.getString(PROTOCOL).equalsIgnoreCase(SNMP)) {

                        if (userFields.size() >= validateFields.size()) {

                            Iterator<Map.Entry<String, Object>> iterator = user.iterator();

                            while (iterator.hasNext()) {

                                if (!validateFields.contains(iterator.next().getKey())) {

                                    iterator.remove();

                                }
                            }

                            for (Map.Entry<String, Object> entry : user) {

                                if (entry.getValue() instanceof String) {

                                    user.put(entry.getKey(), entry.getValue().toString().trim());

                                }
                            }

                            routingContext.setBody(user.toBuffer());

                            routingContext.next();


                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());

                        }

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_PROTOCOL).encodePrettily());

                    }

                } else if (routingContext.request().method() == HttpMethod.PUT) {

                    vertx.eventBus().<JsonArray>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, DATABASE_GET).put(TABLE_NAME, CREDENTIAL_TABLE).put(TABLE_COLUMN, CREDENTIAL_ID).put(TABLE_ID, routingContext.pathParam("id")), handler -> {

                        if (handler.succeeded()) {

                            JsonArray objects = handler.result().body();

                            if (objects.size() == 1) {

                                JsonObject data = objects.getJsonObject(0);

                                if (data.containsKey(PROTOCOL)) {

                                    if (data.getString(PROTOCOL).equalsIgnoreCase(SSH) || data.getString(PROTOCOL).equalsIgnoreCase(WINRM)) {

                                        Iterator<Map.Entry<String, Object>> iterator = user.iterator();

                                        while (iterator.hasNext()) {

                                            if (!checkFields.contains(iterator.next().getKey())) {

                                                iterator.remove();

                                            }
                                        }

                                        for (Map.Entry<String, Object> entry : user) {

                                            if (entry.getValue() instanceof String) {

                                                user.put(entry.getKey(), entry.getValue().toString().trim());

                                            }
                                        }

                                        user.put(CREDENTIAL_ID, routingContext.pathParam("id"));

                                        routingContext.setBody(user.toBuffer());

                                        routingContext.next();


                                    } else {

                                        JsonObject updatedUser = new JsonObject();

                                        for (String field : userFields) {

                                            if (validateFields.contains(field)) {

                                                updatedUser.put(field, user.getValue(field));

                                            }

                                        }

                                        updatedUser.put(CREDENTIAL_ID, user.getValue(CREDENTIAL_ID));

                                        routingContext.setBody(updatedUser.toBuffer());

                                        routingContext.next();

                                    }

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());

                                }

                            } else {

                                routingContext.response()

                                        .setStatusCode(400)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, NOT_PRESENT).encodePrettily());

                            }

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    });

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_REQUEST).encodePrettily());

                }

            } else {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

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

                if (userData != null && !userData.isEmpty()) {

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

                                LOG.debug("Error {}", exception.getMessage());

                                routingContext.response()

                                        .setStatusCode(500)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());

                            }

                        });


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, "Invalid request").encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.MISSING_DATA).encodePrettily());

                }

            } else if (routingContext.request().method() == HttpMethod.GET) {

                JsonObject request = new JsonObject();

                request.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                request.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

                request.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

                request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                    try {

                        if (handler.succeeded()) {

                            routingContext.next();

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());
                    }

                });


            } else if (routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject request = new JsonObject();

                request.put(Constants.METHOD, Constants.CREDENTIAL_DELETE_NAME_CHECK);

                request.put("id", routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                    try {

                        if (handler.succeeded()) {

                            routingContext.next();

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());
                    }

                });

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());

        }

    }

    private void create(RoutingContext routingContext) {

        JsonObject user = routingContext.getBodyAsJson();

        user.put(Constants.METHOD, Constants.DATABASE_INSERT);

        user.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, user, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, response.result().body().getInteger(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void get(RoutingContext routingContext) {

        JsonObject request = new JsonObject();

        request.put(Constants.METHOD, Constants.DATABASE_GET);

        request.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        request.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        request.put(Constants.TABLE_ID, Constants.GETALL);

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS,SUCCESS).put(RESULT,result).encodePrettily());


                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void getId(RoutingContext routingContext) {

        JsonObject request = new JsonObject();

        request.put(Constants.METHOD, Constants.DATABASE_GET);

        request.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        request.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, result).encodePrettily());

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    private void delete(RoutingContext routingContext) {

        JsonObject request = new JsonObject();

        request.put(Constants.METHOD, Constants.DATABASE_DELETE);

        request.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        request.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void update(RoutingContext routingContext) {

        JsonObject user = routingContext.getBodyAsJson();

        user.put(Constants.METHOD, Constants.DATABASE_UPDATE);

        user.put(Constants.TABLE_NAME, Constants.CREDENTIAL_TABLE);

        user.put(Constants.TABLE_COLUMN, Constants.CREDENTIAL_ID);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, user, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

}
