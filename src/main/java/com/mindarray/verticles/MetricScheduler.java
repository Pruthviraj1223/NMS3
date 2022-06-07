package com.mindarray.verticles;

import com.mindarray.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.mindarray.Constants.*;

public class MetricScheduler extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MetricScheduler.class.getName());

    private final HashMap<Integer, Integer> metrics = new HashMap<>();

    private final HashMap<Integer, Integer> updatedMetrics = new HashMap<>();

    private final DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void start(Promise<Void> startPromise) {

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, new JsonObject().put(Constants.METHOD, Constants.DATABASE_GET).put(Constants.TABLE_NAME, Constants.USER_METRIC).put(Constants.TABLE_COLUMN, Constants.METRIC_ID).put(Constants.TABLE_ID, Constants.GETALL), contextHandler -> {

            try {

                if (contextHandler.succeeded()) {

                    if (contextHandler.result().body() != null) {

                        JsonArray metric = contextHandler.result().body();

                        for (int index = 0; index < metric.size(); index++) {

                            var data = metric.getJsonObject(index);

                            if (data.containsKey(METRIC_ID) && data.containsKey(TIME)) {

                                metrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                                updatedMetrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                            }

                        }
                    }

                } else {

                    LOG.debug("Error fail in creating context Initially  {}", contextHandler.cause().getMessage());

                }

            } catch (Exception exception) {

                LOG.debug("Error fail in creating context initially {} ", exception.getMessage());

            }

        });

        vertx.eventBus().<JsonArray>localConsumer(Constants.SCHEDULER, handler -> {

            try {

                if (handler.body() != null) {

                    JsonArray metric = handler.body();

                    for (int index = 0; index < metric.size(); index++) {

                        var data = metric.getJsonObject(index);

                        if (data.containsKey(METRIC_ID) & data.containsKey(TIME)) {

                            metrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                            updatedMetrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                        }

                    }
                }

            } catch (Exception exception) {

                LOG.debug("Error in Scheduler {}", exception.getMessage());

            }

        });

        vertx.setPeriodic(10000, handler -> {

            try {

                for (Map.Entry<Integer, Integer> entry : updatedMetrics.entrySet()) {

                    int time = entry.getValue();

                    time = time - 10000;

                    if (time <= 0) {

                        vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE, new JsonObject().put(Constants.METHOD, Constants.CREATE_CONTEXT).put(Constants.METRIC_ID, entry.getKey()), contextHandler -> {

                            if (contextHandler.succeeded()) {

                                LocalDateTime myDateObj = LocalDateTime.now();

                                String formattedDate = myDateObj.format(myFormatObj);

                                updatedMetrics.put(entry.getKey(), metrics.get(entry.getKey()));

                                vertx.eventBus().send(Constants.EVENTBUS_POLLER, contextHandler.result().body().put(Constants.TIMESTAMP, formattedDate));

                            } else {

                                LOG.debug("Error in set periodic {}", contextHandler.cause().getMessage());

                            }

                        });

                    } else {

                        updatedMetrics.put(entry.getKey(), time);

                    }

                }

            } catch (Exception exception) {

                LOG.debug("Error in set periodic {}", exception.getMessage());

            }

        });

        vertx.eventBus().<JsonArray>localConsumer(Constants.SCHEDULER_DELETE, handler -> {

            try {

                if (handler.body() != null) {

                    JsonArray metricId = handler.body();

                    for (int index = 0; index < metricId.size(); index++) {

                        var data = metricId.getJsonObject(index);

                        if (data.containsKey(METRIC_ID)) {

                            metrics.remove(data.getInteger(Constants.METRIC_ID));

                            updatedMetrics.remove(data.getInteger(Constants.METRIC_ID));

                        }

                    }

                }

            } catch (Exception exception) {

                LOG.debug("Error SCHEDULER DELETE {} ", exception.getMessage());

            }

        });

        vertx.eventBus().<JsonObject>localConsumer(SCHEDULER_UPDATE, handler -> {

            try {

                if (handler.body() != null) {

                    JsonObject user = handler.body();

                    if (user.containsKey(METRIC_ID) && user.containsKey(TIME)) {

                        metrics.replace(user.getInteger(METRIC_ID), user.getInteger(TIME));

                        updatedMetrics.replace(user.getInteger(METRIC_ID), user.getInteger(TIME));

                    }

                }

            } catch (Exception exception) {

                LOG.debug("Error SCHEDULER UPDATE {} ", exception.getMessage());

            }

        });

        startPromise.complete();

    }
}
