import io.vertx.core.json.JsonObject;

public class Testing {

    public static void main(String[] args) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.stream().forEach(val -> {
           jsonObject.getValue(val.getKey());
        });
    }
}
