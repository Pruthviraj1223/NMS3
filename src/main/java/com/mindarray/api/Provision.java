package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import static com.mindarray.Constants.*;

public class Provision  {

    Vertx vertx = Bootstrap.vertx;

    public void init(Router provisionRouter){

        provisionRouter.post("/provision").handler(this::validate).handler(this::insertMetric);


    }


    public void validate(RoutingContext routingContext){

        JsonObject data = routingContext.getBodyAsJson();

        if(data!=null){

            if(data.containsKey(CREDENTIAL_ID) && data.containsKey(IP_ADDRESS) && data.containsKey(TYPE) && data.containsKey(PORT)){

                data.put(METHOD,VALIDATE_PROVISION);

                vertx.eventBus().request(EVENTBUS_DATABASE,data,handler ->{

                    if(handler.succeeded()){

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS,"you got it bro").encodePrettily());


                    }else{

                        routingContext.response()

                                .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                                .end(new JsonObject().put(STATUS,FAIL).put(ERROR,handler.cause().getMessage()).encodePrettily());

                    }

                });


            }else{

                routingContext.response()

                        .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                        .end(new JsonObject().put(STATUS,FAIL).put(ERROR,INVALID_INPUT).encodePrettily());

            }

        }else{


            routingContext.response()

                    .putHeader(Constants.CONTENT_TYPE,Constants.CONTENT_VALUE)

                    .end(new JsonObject().put(STATUS,FAIL).put(ERROR,INVALID_INPUT).encodePrettily());

        }



    }

    public void insertMetric(RoutingContext routingContext){




    }

}
