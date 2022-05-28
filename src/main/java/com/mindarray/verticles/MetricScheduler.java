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

public class MetricScheduler extends AbstractVerticle {

    public static final Logger LOG = LoggerFactory.getLogger(MetricScheduler.class.getName());

    private final HashMap<Integer, Integer> metrics = new HashMap<>();

    private final HashMap<Integer, Integer> updatedMetrics = new HashMap<>();

    @Override
    public void start(Promise<Void> startPromise) {

        vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE, new JsonObject().put(Constants.METHOD, Constants.DATABASE_GET).put(Constants.TABLE_NAME, Constants.USER_METRIC).put(Constants.TABLE_COLUMN, Constants.METRIC_ID).put(Constants.TABLE_ID, Constants.GETALL), contextHandler -> {

            try {

                if (contextHandler.succeeded()) {

                    if (contextHandler.result().body() != null) {

                        JsonArray metric = contextHandler.result().body();

                        for (int index = 0; index < metric.size(); index++) {

                            var data = metric.getJsonObject(index);

                            metrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                            updatedMetrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                        }
                    }

                } else {

                    LOG.debug("Error {}  {}", "fail in creating context Initially ", contextHandler.cause().getMessage());

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

                        metrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

                        updatedMetrics.put(data.getInteger(Constants.METRIC_ID), data.getInteger(Constants.TIME));

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

                                DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                                String formattedDate = myDateObj.format(myFormatObj);

                                vertx.eventBus().send(Constants.EVENTBUS_POLLER, contextHandler.result().body().put("timestamp", formattedDate));

                                updatedMetrics.put(entry.getKey(), metrics.get(entry.getKey()));

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

                        metrics.remove(data.getInteger(Constants.METRIC_ID));

                        updatedMetrics.remove(data.getInteger(Constants.METRIC_ID));

                    }
                    
                }
                
            } catch (Exception exception) {

                LOG.debug("Error SCHEDULER DELETE {} ", exception.getMessage());

            }

        });

        startPromise.complete();

    }
}
