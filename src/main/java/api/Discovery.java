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


public class Discovery {

    Vertx vertx = Bootstrap.vertx;

    public void init(Router discoveryRouter){

        discoveryRouter.post("/discovery").handler(this::validate).handler(this::create);

        discoveryRouter.get("/discovery").handler(this::validate).handler(this::get);

        discoveryRouter.get("/discovery/:id").handler(this::validate).handler(this::getId);

        discoveryRouter.put("/discovery").handler(this::validate).handler(this::update);

        discoveryRouter.delete("/discovery/:id").handler(this::validate).handler(this::delete);

    }

    void validate(RoutingContext routingContext){

        try {

            if (routingContext.request().method() == HttpMethod.POST || routingContext.request().method() == HttpMethod.PUT) {

                HashMap<String, Object> result;

                JsonObject userData = routingContext.getBodyAsJson();

                if (userData != null) {

                    result = new HashMap<>(userData.getMap());

                    for (String key : result.keySet()) {

                        Object val = result.get(key);

                        if (val instanceof String) {

                            result.put(key, val.toString().trim());

                        }

                    }

                    userData = new JsonObject(result);


                    if(routingContext.request().method() == HttpMethod.POST) {

                        vertx.eventBus().<JsonObject>request(Constants.DATABASE_DISCOVERY_CHECK_NAME, userData, handler -> {

                            if (handler.succeeded()) {

                                JsonObject response = handler.result().body();

                                routingContext.setBody(response.toBuffer());

                                routingContext.next();

                            } else {

                                routingContext.response()

                                        .setStatusCode(400)

                                        .putHeader(Constants.CONTENT_TYPE, "application/json")

                                        .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                            }

                        });
                    }else{

                        routingContext.next();

                    }

                } else {

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

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

    void create(RoutingContext routingContext){

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_DISCOVERY_INSERT, userData, response -> {

            try {
                if (response.succeeded()) {

                    JsonObject result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.DISCOVERY_TABLE_ID, result.getString(Constants.DISCOVERY_TABLE_ID)).encodePrettily());

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

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_DISCOVERY_GET_ALL, "", response -> {

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

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_DISCOVERY_GET_ID, id, response -> {

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


    void update(RoutingContext routingContext) {

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_DISCOVERY_UPDATE, userData, response -> {

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

    void delete(RoutingContext routingContext) {

        String id = routingContext.pathParam("id");

        vertx.eventBus().request(Constants.DATABASE_DISCOVERY_DELETE, id, response -> {

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


}
