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

public class Discovery {

    private static final Logger LOG = LoggerFactory.getLogger(Discovery.class.getName());

    private final Vertx vertx = Bootstrap.vertx;

    private final Set<String> checkFields = Set.of(CREDENTIAL_ID, DISCOVERY_NAME, IP_ADDRESS, TYPE, PORT);

    public void init(Router discoveryRouter) {

        discoveryRouter.post("/discovery").handler(this::filter).handler(this::validate).handler(this::create);

        discoveryRouter.get("/discovery").handler(this::get);

        discoveryRouter.get("/discovery/:id").handler(this::validate).handler(this::getId);

        discoveryRouter.put("/discovery/:id").handler(this::filter).handler(this::validate).handler(this::update);

        discoveryRouter.delete("/discovery/:id").handler(this::validate).handler(this::delete);

        discoveryRouter.post("/discovery/run/:id").handler(this::validate).handler(this::merge).handler(this::runDiscovery);

    }

    private void filter(RoutingContext routingContext) {

        try {

            JsonObject user = routingContext.getBodyAsJson();

            if (user != null && !user.isEmpty()) {

                for (Map.Entry<String, Object> entry : user) {

                    if (entry.getValue() instanceof String) {

                        user.put(entry.getKey(), entry.getValue().toString().trim());

                    }
                }

                Set<String> fieldNames = user.fieldNames();

                if (routingContext.request().method() == HttpMethod.POST) {

                    if (fieldNames.size() >= checkFields.size()) {

                        Iterator<Map.Entry<String, Object>> iterator = user.iterator();

                        while (iterator.hasNext()) {

                            if (!checkFields.contains(iterator.next().getKey())) {

                                iterator.remove();

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

                } else if (routingContext.request().method() == HttpMethod.PUT) {

                    Iterator<Map.Entry<String, Object>> iterator = user.iterator();

                    while (iterator.hasNext()) {

                        if (!checkFields.contains(iterator.next().getKey())) {

                            iterator.remove();

                        }
                    }

                    // ## 4

                    user.put(DISCOVERY_TABLE_ID, routingContext.pathParam("id"));

                    routingContext.setBody(user.toBuffer());

                    routingContext.next();

                }

            } else {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", (Object) exception.getStackTrace());

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());

        }

    }

    private void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST) {

                JsonObject userData = routingContext.getBodyAsJson();

                if (userData != null && !routingContext.request().params().isEmpty()) {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());

                } else if (!routingContext.request().params().isEmpty()) {

                    JsonObject request = new JsonObject();

                    request.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                    request.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                    request.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                    request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

                    vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                        try {

                            if (handler.succeeded()) {

                                routingContext.next();

                            } else {

                                routingContext.response()

                                        .setStatusCode(400)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.NOT_PRESENT).encodePrettily());

                            }

                        } catch (Exception exception) {

                            LOG.debug("Error {}", (Object) exception.getStackTrace());

                            routingContext.response()

                                    .setStatusCode(500)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, FAIL).encodePrettily());
                        }

                    });

                } else {

                    if (userData != null && !userData.isEmpty()) {

                        if ((userData.containsKey(Constants.CREDENTIAL_ID) && userData.containsKey(Constants.DISCOVERY_NAME) && userData.containsKey(Constants.PORT) && userData.containsKey(Constants.TYPE) && userData.containsKey(Constants.IP_ADDRESS))) {

                            userData.put(Constants.METHOD, DISCOVERY_POST_CHECK);

                            vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                                try {

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

                                } catch (Exception exception) {

                                    LOG.debug("Error {}", (Object) exception.getStackTrace());

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

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());

                        }

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.INVALID_INPUT).encodePrettily());

                    }
                }

            } else if (routingContext.request().method() == HttpMethod.PUT) {

                JsonObject userData = routingContext.getBodyAsJson();

                userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                userData.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                userData.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

                userData.put(Constants.TABLE_ID, userData.getValue(Constants.DISCOVERY_TABLE_ID));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                    try {

                        if (handler.succeeded()) {

                            userData.put(RESULT, "{}");

                            routingContext.setBody(userData.toBuffer());

                            routingContext.next();

                        } else {

                            routingContext.response()

                                    .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                    .end(new JsonObject().put(STATUS, FAIL).put(ERROR, NOT_PRESENT).encodePrettily());

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", (Object) exception.getStackTrace());

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, exception.getMessage()).encodePrettily());

                    }

                });

            } else if (routingContext.request().method() == HttpMethod.DELETE || routingContext.request().method() == HttpMethod.GET) {

                JsonObject request = new JsonObject();

                request.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                request.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

                request.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

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

                        LOG.debug("Error {} ", (Object) exception.getStackTrace());

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, exception.getMessage()).encodePrettily());

                    }

                });

            } else {

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(ERROR, INVALID_REQUEST).encodePrettily());

            }

        } catch (Exception exception) {

            LOG.debug("Error {} ", (Object) exception.getStackTrace());

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

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

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.DISCOVERY_TABLE_ID, response.result().body().getInteger(Constants.DISCOVERY_TABLE_ID)).encodePrettily());

                    } else {

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

                routingContext.response()

                        .setStatusCode(500)

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

                        JsonArray result = response.result().body();

                        routingContext.response()

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, SUCCESS).put(RESULT, result).encodePrettily());


                    } else {

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                    }


                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

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

        request.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        request.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

        request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

            try {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        JsonArray result = response.result().body();

                        routingContext.response()

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(result.encodePrettily());

                    } else {

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

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

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

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

        request.put(Constants.TABLE_NAME, Constants.DISCOVERY_TABLE);

        request.put(Constants.TABLE_COLUMN, Constants.DISCOVERY_TABLE_ID);

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

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void merge(RoutingContext routingContext) {

        JsonObject request = new JsonObject();

        request.put(Constants.METHOD, MERGE_DATA);

        request.put("id", routingContext.pathParam("id"));

        vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, request, handler -> {

            try {

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

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void runDiscovery(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(RUN_DISCOVERY, userData, response -> {

            try {

                if (response.succeeded()) {

                    if (response.result().body() != null) {

                        userData.remove(PORT);

                        userData.remove(IP_ADDRESS);

                        userData.remove(USERNAME);

                        userData.remove(PASSWORD);

                        userData.remove(COMMUNITY);

                        userData.remove(VERSION);

                        userData.remove(TYPE);

                        userData.put(METHOD, RUN_DISCOVERY_INSERT);

                        userData.put(RESULT, response.result().body());

                        vertx.eventBus().request(EVENTBUS_DATABASE, userData, handler -> {

                            if (handler.succeeded()) {

                                routingContext.response()

                                        .setStatusCode(200)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(response.result().body().encodePrettily());

                            } else {

                                routingContext.response()

                                        .setStatusCode(400)

                                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                        .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                            }

                        });

                    } else {

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                    }


                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error {}", (Object) exception.getStackTrace());

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

}
