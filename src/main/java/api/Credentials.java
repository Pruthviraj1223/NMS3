package api;

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

        credentialRouter.get("/credentials").handler(this::validate).handler(this::get);

        credentialRouter.get("/credentials/:id").handler(this::validate).handler(this::getId);

        credentialRouter.put("/credentials").handler(this::validate).handler(this::update);

        credentialRouter.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    void validate(RoutingContext routingContext) {

        try {

            boolean flag = true;

            if (routingContext.request().method() == HttpMethod.POST || routingContext.request().method() == HttpMethod.PUT) {

                JsonObject userData = routingContext.getBodyAsJson();

                if (userData != null) {

                    if(userData.containsKey(Constants.CREDENTIAL_NAME) && userData.containsKey(Constants.PROTOCOL)){

                        if(userData.getString(Constants.PROTOCOL).equalsIgnoreCase(Constants.SSH) || userData.getString(Constants.PROTOCOL).equalsIgnoreCase(Constants.WINRM)){

                            if(!(userData.containsKey(Constants.NAME) && userData.containsKey(Constants.PASSWORD))){

                                flag = false;

                            }

                        }else if(userData.getString(Constants.PROTOCOL).equalsIgnoreCase(Constants.SNMP)){

                            if(!(userData.containsKey(Constants.COMMUNITY) && userData.containsKey(Constants.VERSION))){

                                flag = false;

                            }

                        } else {

                            flag = false;

                        }

                    }else{

                        flag =false;

                    }

                    if(flag){

                        HashMap<String, Object> result;

                        result = new HashMap<>(userData.getMap());

                        for (String key : result.keySet()) {

                            Object val = result.get(key);

                            if (val instanceof String) {

                                result.put(key, val.toString().trim());

                            }

                        }

                        userData = new JsonObject(result);

                        if(routingContext.request().method() == HttpMethod.POST){

                            vertx.eventBus().<JsonObject>request(Constants.DATABASE_CHECK_NAME, userData, handler -> {

                                if (handler.succeeded()) {

                                    JsonObject response = handler.result().body();

                                    routingContext.setBody(response.toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.EXIST).encodePrettily());

                                }

                            });
                        } else{

                            routingContext.next();

                        }


                    }else{

                        routingContext.response()

                                .setStatusCode(400)

                                .putHeader(Constants.CONTENT_TYPE, "application/json")

                                .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR,Constants.MISSING_DATA).encodePrettily());

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR,Constants.MISSING_DATA).encodePrettily());

                }
            } else if (routingContext.request().method() == HttpMethod.GET || routingContext.request().method() == HttpMethod.DELETE) {

                routingContext.next();

            }

        } catch (Exception exception) {

            routingContext.response()

                    .setStatusCode(400)

                    .putHeader(Constants.CONTENT_TYPE, "application/json")

                    .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());
        }
    }


    void create(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_CREDENTIAL_INSERT, userData, response -> {

            try {
                if (response.succeeded()) {

                    JsonObject result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, result.getString(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR,Constants.INVALID_INPUT).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void get(RoutingContext routingContext) {

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_CREDENTIAL_GET_ALL, "", response -> {

            try {
                if (response.succeeded()) {

                    JsonArray jsonArray = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(jsonArray.encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void getId(RoutingContext routingContext) {

        String id = routingContext.pathParam("id");

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_CREDENTIAL_GET_ID, id, response -> {

            try {
                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(result.encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void delete(RoutingContext routingContext) {

        String id = routingContext.pathParam("id");

        vertx.eventBus().request(Constants.DATABASE_CREDENTIAL_DELETE, id, response -> {

            try {
                if (response.succeeded()) {

//                    JsonArray jsonArray = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });


    }

    void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_CREDENTIAL_UPDATE, userData, response -> {

            try {
                if (response.succeeded()) {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            } catch (Exception exception) {

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

            }

        });

    }

}
