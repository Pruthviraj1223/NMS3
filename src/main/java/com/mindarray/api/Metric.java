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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.mindarray.Constants.*;

public class Metric {

    private static final Logger LOG = LoggerFactory.getLogger(Metric.class.getName());

    private final Vertx vertx = Bootstrap.vertx;

    private final HashSet<String> checkFields = new HashSet<>(Arrays.asList(TYPE, METRIC_GROUP, TIME));

    private final Set<String> checkMetricGroupSNMP = Set.of("SystemInfo", "interface");

    private final Set<String> checkMetricGroupOthers = Set.of("cpu", "disk", "memory", "SystemInfo", "process");


    public void init(Router router) {

        router.get("/metric/").handler(this::getAll);

        router.get("/metric/:id").handler(this::validate).handler(this::getByIdMetric);

        router.get("/metric/monitor/:id").handler(this::validate).handler(this::getByIdMonitor);

        router.put("/metric/:id").handler(this::validate).handler(this::update);

    }

    public void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.PUT) {

                if (routingContext.getBodyAsJson() != null) {

                    String id = routingContext.pathParam("id");

                    JsonObject userData = routingContext.getBodyAsJson();

                    HashMap<String, Object> result = new HashMap<>(userData.getMap());

                    for (String key : result.keySet()) {

                        Object val = result.get(key);

                        if (val instanceof String) {

                            userData.put(key, val.toString().trim());

                        }

                    }

                    Set<String> fieldNames = userData.fieldNames();

                    if (fieldNames.size() >= checkFields.size()) {

                        JsonObject updatedUser = new JsonObject();

                        for (String field : fieldNames) {

                            if (checkFields.contains(field)) {

                                updatedUser.put(field, userData.getValue(field));

                            }

                        }

                        updatedUser.put(MONITOR_ID, id);

                        if (updatedUser.getValue(METRIC_GROUP) instanceof String && updatedUser.getValue(TIME) instanceof Integer) {

                            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, new JsonObject().put(METHOD, DATABASE_ID_CHECK).put(TABLE_NAME, USER_METRIC).put(TABLE_COLUMN, MONITOR_ID).put(TABLE_ID, updatedUser.getValue(MONITOR_ID)), handler -> {

                                if (handler.succeeded()) {

                                    vertx.eventBus().<JsonArray>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, DATABASE_GET).put(TABLE_NAME, MONITOR).put(TABLE_COLUMN, MONITOR_ID).put(TABLE_ID, updatedUser.getValue(MONITOR_ID)), response -> {

                                        if (response.succeeded() && response.result().body().size() == 1) {

                                            var user = response.result().body();

                                            var type = user.getJsonObject(0).getString(TYPE);

                                            if (type != null && type.equalsIgnoreCase(NETWORKING)) {

                                                if (checkMetricGroupSNMP.contains(updatedUser.getString(METRIC_GROUP))) {

                                                    if (MIN_POLL_TIME <= updatedUser.getInteger(TIME) && updatedUser.getInteger(TIME) <= MAX_POLL_TIME) {

                                                        routingContext.setBody(updatedUser.toBuffer());

                                                        routingContext.next();

                                                    } else {

                                                        routingContext.response()

                                                                .setStatusCode(400)

                                                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid time").encodePrettily());

                                                    }

                                                } else {

                                                    routingContext.response()

                                                            .setStatusCode(400)

                                                            .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid metric group").encodePrettily());

                                                }

                                            } else if (type!=null && (type.equalsIgnoreCase(LINUX) || type.equalsIgnoreCase(WINDOWS))) {

                                                if (checkMetricGroupOthers.contains(updatedUser.getString(METRIC_GROUP))) {

                                                    if (MIN_POLL_TIME <= updatedUser.getInteger(TIME) && updatedUser.getInteger(TIME) <= MAX_POLL_TIME) {

                                                        routingContext.setBody(updatedUser.toBuffer());

                                                        routingContext.next();

                                                    } else {

                                                        routingContext.response()

                                                                .setStatusCode(400)

                                                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid time").encodePrettily());

                                                    }

                                                } else {

                                                    routingContext.response()

                                                            .setStatusCode(400)

                                                            .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid metric group").encodePrettily());

                                                }

                                            } else {

                                                routingContext.response()

                                                        .setStatusCode(400)

                                                        .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                        .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid Type").encodePrettily());

                                            }

                                        } else {

                                            routingContext.response()

                                                    .setStatusCode(400)

                                                    .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                    .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                                        }

                                    });

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                                }

                            });

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                    .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

                        }


                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());

                }

            } else if (routingContext.request().method() == HttpMethod.GET || routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject request = new JsonObject();

                request.put(METHOD, DATABASE_ID_CHECK);

                request.put(TABLE_NAME, USER_METRIC);

                request.put(TABLE_COLUMN, METRIC_ID);

                request.put(TABLE_ID, routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                    try {

                        if (handler.succeeded()) {

                            routingContext.next();

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                    .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    } catch (Exception exception) {

                        routingContext.response()

                                .setStatusCode(500)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                    }

                });
            }


        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }

    }

    private void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(METHOD, DATABASE_UPDATE_GROUP_TIME);

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

                routingContext.response()

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void getAll(RoutingContext routingContext) {

        try {

            JsonObject request = new JsonObject();

            request.put(METHOD, Constants.DATABASE_GET);

            request.put(TABLE_NAME, USER_METRIC);

            request.put(TABLE_COLUMN, METRIC_ID);

            request.put(TABLE_ID, GETALL);

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, Constants.NOT_PRESENT).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }


    }

    private void getByIdMetric(RoutingContext routingContext) {

        try {

            JsonObject request = new JsonObject();

            request.put(Constants.METHOD, Constants.DATABASE_GET);

            request.put(Constants.TABLE_NAME, USER_METRIC);

            request.put(Constants.TABLE_COLUMN, METRIC_ID);

            request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, Constants.NOT_PRESENT).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

                }

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }


    }

    private void getByIdMonitor(RoutingContext routingContext) {

        try {

            JsonObject request = new JsonObject();

            request.put(Constants.METHOD, Constants.DATABASE_GET);

            request.put(Constants.TABLE_NAME, USER_METRIC);

            request.put(Constants.TABLE_COLUMN, MONITOR_ID);

            request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, request, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .setStatusCode(200)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, Constants.NOT_PRESENT).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, response.cause().getMessage()).encodePrettily());

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
