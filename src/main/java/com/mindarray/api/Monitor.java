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

import java.util.HashMap;

import java.util.List;

import static com.mindarray.Constants.*;

public class Monitor {

    static final Logger LOG = LoggerFactory.getLogger(Monitor.class.getName());
    Vertx vertx = Bootstrap.vertx;

    public void init(Router provisionRouter) {

        provisionRouter.post("/provision").handler(this::validate).handler(this::insertMonitor).handler(this::snmpInterface).handler(this::insertMetric);

        provisionRouter.get().handler(this::getAll);

        provisionRouter.get("/:id").handler(this::validate).handler(this::getById);

        provisionRouter.delete("/:id").handler(this::validate).handler(this::delete);

    }

    public void validate(RoutingContext routingContext) {

        if (routingContext.request().method() == HttpMethod.POST) {

            JsonObject data = routingContext.getBodyAsJson();

            if (data != null) {

                HashMap<String, Object> result;

                result = new HashMap<>(data.getMap());

                // this can be modified

                for (String key : result.keySet()) {

                    Object val = result.get(key);

                    if (val instanceof String) {

                        data.put(key, val.toString().trim());

                    }

                }

                if (data.containsKey(CREDENTIAL_ID) && data.containsKey(IP_ADDRESS) && data.containsKey(TYPE) && data.containsKey(PORT) && data.containsKey(HOST)) {

                    data.put(METHOD, VALIDATE_PROVISION);

                    vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {

                        if (handler.succeeded()) {

                            routingContext.setBody(handler.result().body().toBuffer());

                            routingContext.next();


                        } else {

                            routingContext.response()

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    });


                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

                }

            } else {

                routingContext.response()

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

            }
        } else if (routingContext.request().method() == HttpMethod.GET || routingContext.request().method() == HttpMethod.DELETE) {

            JsonObject userData = new JsonObject();

            userData.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

            userData.put(Constants.TABLE_NAME, MONITOR);

            userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

            userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, handler -> {

                if (handler.succeeded()) {

                    routingContext.next();

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, NOT_PRESENT).encodePrettily());

                }

            });

        }

    }

    private void insertMonitor(RoutingContext routingContext) {

        JsonObject data = routingContext.getBodyAsJson();

        data.put(METHOD, DATABASE_INSERT);

        data.put(TABLE_NAME, MONITOR);

        vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {

            if (handler.succeeded()) {

//                data.put(MONITOR_ID, handler.result().body().getInteger(MONITOR_ID));

                routingContext.response()

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(STATUS, SUCCESS).put(MONITOR_ID, data.getInteger(MONITOR_ID)).encodePrettily());

                routingContext.setBody(data.toBuffer());

                routingContext.next();

            } else {

                routingContext.response()

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

            }

        });

    }

    private void snmpInterface(RoutingContext routingContext) {

        try {

            JsonObject userData = routingContext.getBodyAsJson();

            if (!userData.getString(TYPE).equalsIgnoreCase(NETWORKING)) {

                routingContext.next();

            } else {

                JsonArray interfaces = userData.getJsonObject(OBJECTS).getJsonArray("interface");

                List<JsonObject> list = interfaces.stream().map(JsonObject::mapFrom).filter(val -> val.getString("interface.operational.status").equalsIgnoreCase("Up")).toList();

                userData.put(OBJECTS, userData.getJsonObject(OBJECTS).put("interface", list));

                routingContext.setBody(userData.toBuffer());

                routingContext.next();

            }

        } catch (Exception exception) {

            LOG.error("ERROR {}" , exception.getMessage());

        }
    }

    private void insertMetric(RoutingContext routingContext) {

        JsonObject data = routingContext.getBodyAsJson();

        data.put(METHOD, INSERT_METRIC);

        data.remove(HOST);

        data.remove(IP_ADDRESS);

        data.remove(PORT);

        vertx.eventBus().<JsonArray>request(EVENTBUS_DATABASE, data, handler -> {

            if (handler.succeeded()) {

                System.out.println("inserted in metric " + handler.result().body());

                vertx.eventBus().send(SCHEDULER, handler.result().body());


            } else {

                LOG.debug("Error : Insert Metric {} {} ", "Data is not inserted ", handler.cause().getMessage());

            }

        });

    }

    private void getAll(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, MONITOR);

        userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

        userData.put(Constants.TABLE_ID, "getall");

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

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

    private void getById(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, MONITOR);

        userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

        userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {
                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(result.encodePrettily());


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

    private void delete(RoutingContext routingContext) {

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, DATABASE_DELETE_MONITOR);

        userData.put(TABLE_ID, routingContext.pathParam("id"));

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

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }
}
