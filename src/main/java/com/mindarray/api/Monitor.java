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

import java.util.*;

import static com.mindarray.Constants.*;

public class Monitor {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class.getName());

    private final Set<String> checkFields = new HashSet<>(Arrays.asList(CREDENTIAL_ID, HOST, IP_ADDRESS, TYPE, PORT));

    private final Vertx vertx = Bootstrap.vertx;

    public void init(Router router) {

        router.post("/provision").handler(this::fieldValidate).handler(this::validate).handler(this::insertMonitor).handler(this::snmpInterface).handler(this::insertMetric);

        router.get("/limit").handler(this::getPollingData);

        router.get("/").handler(this::getAll);

        router.get("/:id").handler(this::validate).handler(this::getById);

        router.delete("/:id").handler(this::validate).handler(this::delete);

    }

    private void fieldValidate(RoutingContext routingContext) {

        try {

            if (routingContext.request().method() == HttpMethod.POST) {

                JsonObject user = routingContext.getBodyAsJson();

                if (user != null) {

                    Set<String> fieldNames = user.fieldNames();

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


                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, FAIL).put(ERROR, INVALID_INPUT).encodePrettily());

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

    public void validate(RoutingContext routingContext) {

        try {

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

                                if (handler.result().body() != null) {

                                    routingContext.setBody(handler.result().body().toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(STATUS, FAIL).encodePrettily());

                                }

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

                System.out.println("path " + routingContext.request().params() + " path " + routingContext.request().path()  +" uri " + routingContext.request().uri());

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

                JsonArray interfaces = userData.getJsonObject(OBJECTS).getJsonArray("interface");

                List<JsonObject> list = interfaces.stream().map(JsonObject::mapFrom).filter(val -> val.getString("interface.operational.status").equalsIgnoreCase("Up")).toList();

                userData.put(OBJECTS, userData.getJsonObject(OBJECTS).put("interface", list));

                routingContext.setBody(userData.toBuffer());

                routingContext.next();

            }

        } catch (Exception exception) {

            LOG.error("ERROR {}", exception.getMessage());

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

            LOG.debug("ERROR insert metric {} ", exception.getMessage());

        }

    }

    private void getPollingData(RoutingContext routingContext){

        JsonObject userData = new JsonObject();

        userData.put(METHOD, DATABASE_GET_POLL_DATA);

        userData.put(TABLE_COLUMN, MONITOR_ID);

        userData.put(TABLE_ID,routingContext.queryParam(MONITOR_ID).get(0));

        userData.put(LIMIT, routingContext.queryParam(LIMIT).get(0));

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, userData, response -> {

            try {

                if(response.succeeded()){

                    routingContext.response()

                            .setStatusCode(200)

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(STATUS, SUCCESS).put(RESULT,response.result().body()).encodePrettily());



                }else{

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

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, response.cause().getMessage()).encodePrettily());

                }

            } catch (Exception exception) {

                LOG.debug("Error : Get All {}", exception.getMessage());

                routingContext.response()

                        .setStatusCode(500)

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

            JsonObject userData = new JsonObject();

            userData.put(Constants.METHOD, DATABASE_DELETE_MONITOR);

            userData.put(TABLE_ID, routingContext.pathParam("id"));

            vertx.eventBus().request(Constants.EVENTBUS_DATABASE, userData, response -> {

                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

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
}
