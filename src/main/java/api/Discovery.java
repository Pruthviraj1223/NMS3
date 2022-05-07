package api;

import com.mindarray.Bootstrap;
import io.vertx.core.Vertx;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class Discovery {

    public void init(Router subroute){

        subroute.post("/discovery").setName("POST").handler(this::validate).handler(this::create);

        subroute.get("/discovery").setName("GET").handler(this::validate).handler(this::get);

        subroute.put("/discovery").setName("DELETE").handler(this::validate).handler(this::delete);

        subroute.delete("/discovery").setName("PUT").handler(this::validate).handler(this::update);

    }

    void validate(RoutingContext rx){

        JsonObject jsonObject = rx.getBodyAsJson();

        System.out.println("data in validate " + jsonObject);

        jsonObject.remove("port");

        rx.setBody(jsonObject.toBuffer());

        rx.next();

    }

    void create(RoutingContext cx){

        JsonObject jsonObject = cx.getBodyAsJson();

        System.out.println("data in create " + jsonObject);

        Vertx vertx = Bootstrap.vertx;

        cx.response().end();

    }

    void get(RoutingContext rx){



    }

    void delete(RoutingContext rx){

    }

    void
    update(RoutingContext rx){

    }

}
