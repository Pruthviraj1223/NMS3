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

        credentialRouter.get("/credentials").handler(this::get);

        credentialRouter.get("/credentials/:id").handler(this::validate).handler(this::getId);

        credentialRouter.put("/credentials").handler(this::validate).handler(this::update);

        credentialRouter.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    void validate(RoutingContext routingContext) {

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

                        if(routingContext.request().method() == HttpMethod.POST){

                            vertx.eventBus().<JsonObject>request(Constants.DATABASE_CHECK_NAME, userData, handler -> {

                                if (handler.succeeded()) {

                                    routingContext.setBody(userData.toBuffer());

                                    routingContext.next();

                                } else {

                                    routingContext.response()

                                            .setStatusCode(400)

                                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR, Constants.EXIST).encodePrettily());

                                }

                            });

                        } else{


                            vertx.eventBus().request(Constants.CREDENTIAL_PUT_NAME_CHECK,userData,handler->{

                                if(handler.succeeded()){

                                    routingContext.setBody(userData.toBuffer());

                                    routingContext.next();

                                }else{

                                    routingContext.response()

                                            .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                                            .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).put(Constants.ERROR,Constants.INVALID_INPUT).encodePrettily());

                                }

                            });

                        }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.ERROR,Constants.MISSING_DATA).encodePrettily());

                }
            } else if (routingContext.request().method() == HttpMethod.GET) {

                String id = routingContext.pathParam("id");

                vertx.eventBus().request(Constants.CREDENTIAL_GET_NAME_CHECK,id,handler->{

                    if(handler.succeeded()){

                        routingContext.next();

                    }else{

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).put(Constants.ERROR,Constants.MISSING_DATA).encodePrettily());

                    }

                });


            } else if (routingContext.request().method() == HttpMethod.DELETE) {

                String id = routingContext.pathParam("id");

                vertx.eventBus().request(Constants.CREDENTIAL_DELETE_NAME_CHECK,id,handler->{

                    if(handler.succeeded()){

                        routingContext.next();

                    }else{

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).put(Constants.ERROR,Constants.MISSING_DATA).encodePrettily());

                    }

                });


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

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, result.getString(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

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

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(jsonArray.encodePrettily());

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

    void getId(RoutingContext routingContext) {

        String id = routingContext.pathParam("id");

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_CREDENTIAL_GET_ID, id, response -> {

            try {
                if (response.succeeded()) {

                    JsonArray result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, Constants.CONTENT_VALUE)

                            .end(new JsonObject().put(Constants.STATUS,Constants.SUCCESS).put(Constants.RESULT,result).encodePrettily());

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

    void delete(RoutingContext routingContext) {

        String id = routingContext.pathParam("id");

        vertx.eventBus().request(Constants.DATABASE_CREDENTIAL_DELETE, id, response -> {

            try {
                if (response.succeeded()) {

//                    JsonArray jsonArray = response.result().body();

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

    void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_CREDENTIAL_UPDATE, userData, response -> {

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
