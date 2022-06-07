package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constants;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mindarray.Constants.*;

public class Monitor {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class.getName());

    private final Set<String> checkFields = new HashSet<>(Arrays.asList(CREDENTIAL_ID, HOST, IP_ADDRESS, TYPE, PORT, OBJECTS));

    private final Set<String> checkParams = new HashSet<>(Arrays.asList(MONITOR_ID, LIMIT, METRIC_GROUP));

    private final Set<String> checkMetricGroup = Set.of(CPU, DISK, MEMORY, SYSTEM_INFO, PROCESS, INTERFACE,PING);

    private final Vertx vertx = Bootstrap.vertx;

    public void init(Router router) {

        router.post("/monitor/provision").handler(this::fieldValidate).handler(this::validate).handler(this::insertMonitor).handler(this::snmpInterface).handler(this::insertMetric);

        router.get("/monitor/limit").handler(this::fieldValidate).handler(this::getPollingData);

        router.get("/monitor/").handler(this::getAll);

        router.get("/monitor/:id").handler(this::validate).handler(this::getById);

        router.put("/monitor/:id").handler(this::validate).handler(this::update);

        router.delete("/monitor/:id").handler(this::validate).handler(this::delete);

    }

    private void fieldValidate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST) {

                JsonObject user = routingContext.getBodyAsJson();

                if (user != null && !user.isEmpty()) {

                    Set<String> fieldNames = user.fieldNames();

                    if (fieldNames.size() != 0) {

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

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

                }

            } else if (routingContext.request().method() == HttpMethod.GET){

                JsonObject context = new JsonObject();

                MultiMap map = routingContext.queryParams();

                Set<String> set = map.names();

                for (String field : set) {

                    if (checkParams.contains(field)) {

                        context.put(field, map.get(field));

                    }

                }

                if (context.containsKey(MONITOR_ID)) {

                    JsonObject request = new JsonObject();

                    request.put(METHOD, DATABASE_ID_CHECK);

                    request.put(TABLE_NAME, MONITOR);

                    request.put(TABLE_COLUMN, MONITOR_ID);

                    request.put(TABLE_ID, context.getValue(MONITOR_ID));

                    vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                        if (handler.succeeded()) {

                            if (context.containsKey(METRIC_GROUP)) {

                                if (checkMetricGroup.contains(context.getString(METRIC_GROUP))) {

                                    routingContext.setBody(context.toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_METRIC_GROUP).encodePrettily());
                                }

                            } else {

                                routingContext.setBody(context.toBuffer());

                                routingContext.next();

                            }

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, handler.cause().getMessage()).encodePrettily());

                        }

                    });

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, MONITOR_ID_MISSING).encodePrettily());

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

    public void validate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST || routingContext.request().method() == HttpMethod.PUT) {

                JsonObject user = routingContext.getBodyAsJson();

                if (user != null && !user.isEmpty()) {

                    for (Map.Entry<String, Object> entry : user) {

                        if (entry.getValue() instanceof String) {

                            user.put(entry.getKey(), entry.getValue().toString().trim());

                        }
                    }

                    if (routingContext.request().method() == HttpMethod.POST) {

                        if (user.containsKey(CREDENTIAL_ID) && user.containsKey(IP_ADDRESS) && user.containsKey(TYPE) && user.containsKey(PORT) && user.containsKey(HOST)) {

                            user.put(METHOD, VALIDATE_PROVISION);

                            vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, user, handler -> {

                                if (handler.succeeded()) {

                                    if (handler.result().body() != null) {

                                        routingContext.setBody(handler.result().body().toBuffer());

                                        routingContext.next();

                                    } else {

                                        routingContext.response()

                                                .setStatusCode(400)

                                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                                .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                                    }

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                                }

                            });

                        } else {

                            routingContext.response()

                                    .setStatusCode(400)

                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                    .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

                        }

                    } else {

                        JsonObject request = new JsonObject();

                        request.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                        request.put(Constants.TABLE_NAME, MONITOR);

                        request.put(Constants.TABLE_COLUMN, MONITOR_ID);

                        request.put(Constants.TABLE_ID,  routingContext.pathParam("id"));

                        vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                            try {

                                if (handler.succeeded()) {

                                    if (user.containsKey(PORT)) {

                                        if (user.getValue(PORT) instanceof Integer) {

                                            user.put(MONITOR_ID,  routingContext.pathParam("id"));

                                            routingContext.setBody(user.toBuffer());

                                            routingContext.next();

                                        } else {

                                            routingContext.response()

                                                    .setStatusCode(400)

                                                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, INVALID_PORT).encodePrettily());

                                        }

                                    } else {

                                        routingContext.response()

                                                .setStatusCode(400)

                                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, PORT_MISSING).encodePrettily());

                                    }

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

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

                }

            } else if (routingContext.request().method() == HttpMethod.GET || routingContext.request().method() == HttpMethod.DELETE) {

                JsonObject request = new JsonObject();

                request.put(Constants.METHOD, Constants.DATABASE_ID_CHECK);

                request.put(Constants.TABLE_NAME, MONITOR);

                request.put(Constants.TABLE_COLUMN, MONITOR_ID);

                request.put(Constants.TABLE_ID, routingContext.pathParam("id"));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE, request, handler -> {

                    if (handler.succeeded()) {

                        routingContext.setBody(request.toBuffer());

                        routingContext.next();

                    } else {

                        routingContext.response()

                                .setStatusCode(400)

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

    private void insertMonitor(RoutingContext routingContext) {

        try {

            JsonObject data = routingContext.getBodyAsJson();

            data.put(METHOD, DATABASE_INSERT);

            data.put(TABLE_NAME, MONITOR);

            vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {

                if (handler.succeeded()) {

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, SUCCESS).put(MONITOR_ID, data.getInteger(MONITOR_ID)).encodePrettily());

                    routingContext.setBody(data.toBuffer());

                    routingContext.next();

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, FAIL).put(ERROR, handler.cause().getMessage()).encodePrettily());

                }

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }

    }

    private void snmpInterface(RoutingContext routingContext) {

        try {

            JsonObject userData = routingContext.getBodyAsJson();

            if (!userData.getString(TYPE).equalsIgnoreCase(NETWORKING)) {

                routingContext.next();

            } else {

                if (userData.getJsonObject(OBJECTS).containsKey(INTERFACE)) {

                    JsonArray interfaces = userData.getJsonObject(OBJECTS).getJsonArray(INTERFACE);

                    List<JsonObject> list = interfaces.stream().map(JsonObject::mapFrom).filter(val -> val.getString("interface.operational.status").equalsIgnoreCase("Up")).toList();

                    userData.put(OBJECTS, userData.getJsonObject(OBJECTS).put(INTERFACE, list));

                } else {

                    LOG.debug("Error {}", "Interface is invalid");

                    userData.remove(OBJECTS);

                }

                routingContext.setBody(userData.toBuffer());

                routingContext.next();

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", (Object) exception.getStackTrace());

        }
    }

    private void insertMetric(RoutingContext routingContext) {

        try {

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

        } catch (Exception exception) {

            LOG.debug("Error {}", (Object) exception.getStackTrace());

        }

    }

    private void getPollingData(RoutingContext routingContext) {

        JsonObject context = routingContext.getBodyAsJson();

        context.put(METHOD, DATABASE_GET_POLL_DATA);

        context.put(TABLE_COLUMN, MONITOR_ID);

        context.put(TABLE_ID, context.getString(MONITOR_ID));

        if (context.containsKey(LIMIT)) {

            context.put(LIMIT, context.getValue(LIMIT));

        } else {

            context.put(LIMIT, 10);

        }

        if (context.containsKey(METRIC_GROUP)) {

            context.put(METRIC_GROUP, context.getValue(METRIC_GROUP));

        } else {

            context.put(METRIC_GROUP, GETALL);

        }

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, context, response -> {

            try {

                if (response.succeeded()) {

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, SUCCESS).put(RESULT, response.result().body()).encodePrettily());


                } else {

                    routingContext.response()

                            .setStatusCode(200)

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

        JsonObject userData = new JsonObject();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, MONITOR);

        userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

        userData.put(Constants.TABLE_ID, GETALL);

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, SUCCESS).put(RESULT, result).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, response.cause().getMessage()).encodePrettily());

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

    private void getById(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        userData.put(Constants.METHOD, Constants.DATABASE_GET);

        userData.put(Constants.TABLE_NAME, MONITOR);

        userData.put(Constants.TABLE_COLUMN, MONITOR_ID);

        userData.put(Constants.TABLE_ID, routingContext.pathParam("id"));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if (response.succeeded()) {

                    JsonArray data = response.result().body();

                    if (!data.isEmpty()) {

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.RESULT, data).encodePrettily());

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

                        .setStatusCode(500)

                        .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

    private void delete(RoutingContext routingContext) {

        try {

            JsonObject userData = routingContext.getBodyAsJson();

            userData.put(Constants.METHOD, DATABASE_DELETE_MONITOR);

            userData.put(TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, response -> {

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

            });

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(500)

                    .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

        }

    }

    private void update(RoutingContext routingContext) {

        JsonObject context = routingContext.getBodyAsJson();

        JsonObject userData = new JsonObject();

        userData.put(METHOD, DATABASE_UPDATE);

        userData.put(TABLE_NAME, MONITOR);

        userData.put(TABLE_COLUMN, MONITOR_ID);

        userData.put(MONITOR_ID, context.getValue(MONITOR_ID));

        userData.put(PORT, context.getValue(PORT));

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

}
