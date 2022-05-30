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
import java.util.HashSet;

import static com.mindarray.Constants.*;

public class Metric {

    private static final Logger LOG = LoggerFactory.getLogger(Metric.class.getName());

    private final Vertx vertx = Bootstrap.vertx;

    private final HashSet<String> checkFields = new HashSet<>(Arrays.asList(METRIC_ID, TIME));

    public void init(Router router) {

        router.get("/").handler(this::getAll);

        router.get("/:id").handler(this::validate).handler(this::getByIdMetric);

        router.get("/monitor/:id").handler(this::validate).handler(this::getByIdMonitor);

        router.put("/").handler(this::validate).handler(this::update);

    }

    public void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.PUT) {

                if (routingContext.getBodyAsJson() != null) {

                    JsonObject userData = routingContext.getBodyAsJson();

                    if (checkFields.equals(userData.fieldNames())) {

                        if (userData.getValue(METRIC_ID) instanceof Integer && userData.getValue(TIME) instanceof Integer) {

                            JsonObject data = new JsonObject();

                            data.put(METHOD, DATABASE_ID_CHECK);

                            data.put(TABLE_NAME, USER_METRIC);

                            data.put(TABLE_COLUMN, METRIC_ID);

                            data.put(TABLE_ID, userData.getInteger(METRIC_ID));

                            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, data, handler -> {

                                if (handler.succeeded()) {

                                    if(MIN_POLL_TIME <= userData.getInteger(TIME) && userData.getInteger(TIME)<= MAX_POLL_TIME){

                                        routingContext.next();

                                    } else {

                                        routingContext.response()

                                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid time").encodePrettily());

                                    }

                                } else {

                                    routingContext.response()

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

                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, "Invalid , dont put extra field").encodePrettily());


                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, MISSING_DATA).encodePrettily());

                }

            } else if (routingContext.request().method() == HttpMethod.GET || routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject data = new JsonObject();

                data.put(METHOD, DATABASE_ID_CHECK);

                data.put(TABLE_NAME, USER_METRIC);

                data.put(TABLE_COLUMN, METRIC_ID);

                data.put(TABLE_ID, routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, data, handler -> {

                    if (handler.succeeded()) {

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .putHeader(CONTENT_TYPE, CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                    }

                });
            }


        } catch (Exception exception) {

            LOG.debug("Error {}",exception.getMessage());

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, exception.getMessage()).encodePrettily());

        }

    }

    private void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(METHOD, DATABASE_UPDATE);

        userData.put(TABLE_NAME, USER_METRIC);

        userData.put(TABLE_COLUMN, METRIC_ID);

        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    vertx.eventBus().send(SCHEDULER_UPDATE,userData);

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

    private void getAll(RoutingContext routingContext) {

        try {

            JsonObject userData = new JsonObject();

            userData.put(METHOD, Constants.DATABASE_GET);

            userData.put(TABLE_NAME, USER_METRIC);

            userData.put(TABLE_COLUMN, METRIC_ID);

            userData.put(TABLE_ID, GETALL);

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

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

            JsonObject userData = new JsonObject();

            userData.put(Constants.METHOD, Constants.DATABASE_GET);

            userData.put(Constants.TABLE_NAME, USER_METRIC);

            userData.put(Constants.TABLE_COLUMN, METRIC_ID);

            userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

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

            JsonObject userData = new JsonObject();

            userData.put(Constants.METHOD, Constants.DATABASE_GET);

            userData.put(Constants.TABLE_NAME, USER_METRIC);

            userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

            userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

                if (response.succeeded()) {

                    JsonArray results = response.result().body();

                    if (!results.isEmpty()) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, results).encodePrettily());

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

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }


    }


}
