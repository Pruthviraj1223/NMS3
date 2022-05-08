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

    public void init(Router router){

        router.post("/credentials").handler(this::validate).handler(this::create);

        router.get("/credentials").handler(this::validate).handler(this::get);

        router.put("/credentials").handler(this::validate).handler(this::update);

        router.delete("/credentials/:id").handler(this::validate).handler(this::delete);

    }

    void validate(RoutingContext routingContext){

        try{

            if(routingContext.request().method()== HttpMethod.POST || routingContext.request().method()== HttpMethod.PUT){

                JsonObject userData = routingContext.getBodyAsJson();

                HashMap<String,Object> result;

                if(!(userData==null)){

                    result = new HashMap<>(userData.getMap());

                    for (String key : result.keySet()) {

                        Object val = result.get(key);

                        if(val instanceof String){

                            result.put(key,val.toString().trim());

                        }

                    }

                    userData = new JsonObject(result);

                    routingContext.setBody(userData.toBuffer());

                    routingContext.next();

                }else{

                    routingContext.response()

                            .setStatusCode(400)

                            .putHeader(Constants.CONTENT_TYPE,"application/json")

                            .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());

                }
            }
            else if(routingContext.request().method()== HttpMethod.GET){

                routingContext.next();

            } else if(routingContext.request().method() == HttpMethod.DELETE){

                routingContext.next();

            }


        }catch (Exception exception){

            routingContext.response()

                    .setStatusCode(400)

                    .putHeader(Constants.CONTENT_TYPE,"application/json")

                    .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());
        }



    }

    void create(RoutingContext routingContext){

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_INSERT,userData,response->{

            try {
                if (response.succeeded()) {

                    JsonObject result = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.SUCCESS).put(Constants.CREDENTIAL_ID, result.getString(Constants.CREDENTIAL_ID)).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            }catch (Exception exception){

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE,"application/json")

                        .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());

            }

        });


    }
    void get(RoutingContext routingContext){

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonArray>request(Constants.DATABASE_GET_ALL,userData,response->{

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

            }catch (Exception exception){

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE,"application/json")

                        .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());

            }

        });


    }

    void delete(RoutingContext routingContext){

        String id = routingContext.pathParam("id");

        vertx.eventBus().request(Constants.DATABASE_DELETE,id,response->{

            try {
                if (response.succeeded()) {

//                    JsonArray jsonArray = response.result().body();

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS,Constants.SUCCESS).encodePrettily());

                } else {

                    routingContext.response()

                            .putHeader(Constants.CONTENT_TYPE, "application/json")

                            .end(new JsonObject().put(Constants.STATUS, Constants.FAIL).encodePrettily());

                }

            }catch (Exception exception){

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE,"application/json")

                        .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());

            }

        });


    }

    void update(RoutingContext routingContext){

        JsonObject userData = routingContext.getBodyAsJson();

        vertx.eventBus().<JsonObject>request(Constants.DATABASE_UPDATE,userData,response->{

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

            }catch (Exception exception){

                routingContext.response()

                        .setStatusCode(400)

                        .putHeader(Constants.CONTENT_TYPE,"application/json")

                        .end(new JsonObject().put(Constants.STATUS,Constants.FAIL).encodePrettily());

            }

        });

    }

}
