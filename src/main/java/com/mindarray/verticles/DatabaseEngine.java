package com.mindarray.verticles;

import com.mindarray.Constants;
import com.mindarray.Utils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static com.mindarray.Constants.*;

public class DatabaseEngine extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    private boolean check(String table, String column, String value) {

        boolean isAvailable;

        if (table == null || column == null || value == null) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "select * from " + table + " where " + column + "='" + value + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            isAvailable = resultSet.next();

        } catch (Exception exception) {

            LOG.debug("Error : {}", exception.getMessage());

            return false;

        }

        return isAvailable;

    }

    private int getId(String tableName, String column) {

        int id;

        if (tableName == null || column == null) {

            return -1;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "select max(" + column + ") from " + tableName;

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            resultSet.next();

            id = resultSet.getInt(1) + 1;

        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return -1;

        }

        return id;

    }

    private boolean containsAllCredential(JsonObject data) {

        if (data == null) {

            return false;
        }

        if (getId(CREDENTIAL_TABLE, CREDENTIAL_ID) != -1) {

            data.put(CREDENTIAL_ID, getId(CREDENTIAL_TABLE, CREDENTIAL_ID));

        }

        if (!(data.containsKey(Constants.CREDENTIAL_ID) && (!data.getString(Constants.CREDENTIAL_ID).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.CREDENTIAL_NAME) && (!data.getString(Constants.CREDENTIAL_NAME).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.PROTOCOL) && (!data.getString(Constants.PROTOCOL).isEmpty()))) {

            return false;

        }


        if (data.getString(Constants.PROTOCOL).equalsIgnoreCase(SSH) || data.getString(Constants.PROTOCOL).equalsIgnoreCase(WINRM)) {

            if (!(data.containsKey(Constants.NAME) && (!data.getString(Constants.NAME).isEmpty()))) {

                return false;

            }

            return data.containsKey(Constants.PASSWORD) && (!data.getString(Constants.PASSWORD).isEmpty());

        } else if (data.getString(Constants.PROTOCOL).equalsIgnoreCase(SNMP)) {

            if (!(data.containsKey(Constants.COMMUNITY) && (!data.getString(Constants.COMMUNITY).isEmpty()))) {

                return false;

            }

            return data.containsKey(Constants.VERSION) && (!data.getString(Constants.VERSION).isEmpty());

        } else {

            return false;

        }

    }

    private boolean containsAllDiscovery(JsonObject data) {

        if (data == null) {

            return false;
        }

        if (getId(DISCOVERY_TABLE, DISCOVERY_TABLE_ID) != -1) {

            data.put(DISCOVERY_TABLE_ID, getId(DISCOVERY_TABLE, DISCOVERY_TABLE_ID));

        }

        if (!(data.containsKey(Constants.CREDENTIAL_ID) && (data.getValue(CREDENTIAL_ID) instanceof Integer))) {

            return false;

        }

        if (!(data.containsKey(Constants.DISCOVERY_TABLE_ID))) {

            return false;

        }

        if (!(data.containsKey(Constants.PORT) && (data.getValue(PORT) instanceof Integer))) {

            return false;

        }

        if (!(data.containsKey(Constants.IP_ADDRESS) && (!data.getString(Constants.IP_ADDRESS).isEmpty()))) {

            return false;

        }
        return data.containsKey(Constants.TYPE) && (!data.getString(Constants.TYPE).isEmpty());

    }

    private boolean containsAllMonitor(JsonObject data) {

        if (data == null) {

            return false;

        }

        if (getId(MONITOR, MONITOR_ID) != -1) {

            data.put(MONITOR_ID, getId(MONITOR, MONITOR_ID));

        }

        if (data.containsKey(TYPE)) {

            if (data.getString(TYPE).equalsIgnoreCase(NETWORKING)) {

                return data.containsKey(OBJECTS);

            }

        }

        return true;

    }

    private boolean containsAllUserMetric(JsonObject data) {

        if (data == null) {

            return false;

        }

        return data.containsKey(CREDENTIAL_ID) && data.containsKey(MONITOR_ID) && data.containsKey(METRIC_ID) && data.containsKey(METRIC_GROUP) && data.containsKey(TIME);

    }

    private void createTable() {

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            Statement statement = connection.createStatement();

            statement.executeUpdate("create table if not exists Credentials (credentialId int PRIMARY KEY ,credential_name varchar(255),protocol varchar(255),name varchar(255),password varchar(255),community varchar(255),version varchar(255))");

            statement.executeUpdate("create table if not exists Discovery (discoveryId int PRIMARY KEY ,credentialId int,discovery_name varchar(255),ip varchar(255),type varchar(255),port int,result JSON)");

            statement.executeUpdate("create table if not exists Monitor (monitorId int PRIMARY KEY ,ip varchar(255),type varchar(255),port int,host varchar(255))");

            statement.executeUpdate("create table if not exists UserMetric (metricId int PRIMARY KEY ,monitorId int,credentialId int,metricGroup varchar(255),time int,objects JSON)");

            statement.executeUpdate("create table if not exists Poller (pollerId int PRIMARY KEY AUTO_INCREMENT , monitorId int, metricGroup varchar(255) ,result json,timestamp DATETIME)");


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }
    }

    private JsonObject insert(String tableName, JsonObject userData) {

        JsonObject result = new JsonObject();

        if (tableName == null || userData == null) {

            return result.put(ERROR, "Data is null");

        }

        userData.remove(TABLE_NAME);

        if (!tableName.isEmpty()) {

            if (tableName.equalsIgnoreCase(MONITOR)) {

                userData.remove(OBJECTS);

                userData.remove(CREDENTIAL_ID);

            }
        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            StringBuilder column = new StringBuilder("insert into ").append(tableName).append("(");

            StringBuilder values = new StringBuilder(" values (");

            Map<String, Object> userMap = userData.getMap();

            for (Map.Entry<String, Object> entry : userMap.entrySet()) {

                column.append(entry.getKey().replace(".", "_")).append(",");

                if (entry.getValue() instanceof Integer) {

                    values.append(entry.getValue()).append(",");

                } else {

                    values.append("'").append(entry.getValue()).append("'").append(",");

                }
            }

            column.deleteCharAt(column.length() - 1).append(")");

            values.deleteCharAt(values.length() - 1).append(")");

            column.append(values);

            PreparedStatement preparedStatement = connection.prepareStatement(column.toString());

            preparedStatement.execute();

            result.put(Constants.STATUS, Constants.SUCCESS);


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

            result.put(ERROR, exception.getMessage());

        }

        return result;

    }

    private boolean update(String tableName, String columnName, JsonObject userData) {

        boolean result = true;

        String id;

        if (tableName == null || columnName == null || userData == null) {

            return false;

        }

        if (userData.containsKey(columnName)) {

            id = userData.getString(columnName);

        } else {

            return false;

        }


        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE) && userData.containsKey(CREDENTIAL_NAME)) {

                if (userData.containsKey(CREDENTIAL_NAME) && check(CREDENTIAL_TABLE, CREDENTIAL_TABLE_NAME, userData.getString(CREDENTIAL_NAME))) {

                    return false;

                }

            }

            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && userData.containsKey(Constants.DISCOVERY_NAME)) {

                if (check(DISCOVERY_TABLE, DISCOVERY_TABLE_NAME, userData.getString(DISCOVERY_NAME))) {

                    return false;

                }

            }

            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && userData.containsKey(CREDENTIAL_ID) && !check(CREDENTIAL_TABLE, CREDENTIAL_ID, userData.getString(CREDENTIAL_ID))) {

                return false;

            }

            Map<String, Object> data = userData.getMap();

            if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE)) {

                data.remove(CREDENTIAL_ID);

                data.remove(PROTOCOL);

            }

            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE)) {

                data.remove(TYPE);

                data.remove(DISCOVERY_TABLE_ID);

            }

            if(tableName.equalsIgnoreCase(MONITOR)){ //changes

                data.remove(MONITOR_ID);

            }

            data.remove(TABLE_COLUMN);

            data.remove(TABLE_NAME);

            data.remove(TABLE_ID);

            String query;

            StringBuilder update = new StringBuilder();

            query = "UPDATE " + tableName + " SET ";

            for (Map.Entry<String, Object> entry : data.entrySet()) {

                update.append(entry.getKey().replace(".", "_")).append(" = ").append("'").append(entry.getValue()).append("'").append(",");

            }

            update.deleteCharAt(update.length() - 1);

            String updatedQuery;

            if (!tableName.equalsIgnoreCase(USER_METRIC)) {

                updatedQuery = query + update + " where " + columnName + " = '" + id + "' ;";

            } else {

                updatedQuery = query + update + " where " + columnName + " = '" + id + "' and metricGroup = '" + userData.getString(METRIC_GROUP) + "';";

            }

            PreparedStatement preparedStatement = connection.prepareStatement(updatedQuery);

            preparedStatement.execute();

        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return false;

        }

        return result;

    }

    private JsonArray getAll(String tableName, String column, String id) {

        if (tableName == null || column == null || id == null) {

            return null;

        }

        JsonArray userData;

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            userData = new JsonArray();

            String query;

            if (id.equalsIgnoreCase(GETALL)) {

                query = "select * from " + tableName;

            } else {

                query = "select * from " + tableName + " where " + column + " = '" + id + "'";

            }

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()) {

                JsonObject result = new JsonObject();

                int columnCount = resultSet.getMetaData().getColumnCount();

                for (int i = 1; i <= columnCount; i++) {

                    String columnName = resultSet.getMetaData().getColumnName(i);

                    String newColumnName = columnName.replace("_", ".");

                    if (!(resultSet.getString(columnName) == null)) {

                        result.put(newColumnName, resultSet.getObject(i));

                    }
                }

                userData.add(result);

            }

        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return null;

        }

        return userData;

    }

    private boolean delete(String tableName, String column, String id) {

        boolean result = false;

        if (id == null || tableName == null || column == null) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "delete from " + tableName + " where " + column + " ='" + id + "'";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            if (preparedStatement.executeUpdate() > 0) {

                result = true;

            }

        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return false;

        }

        return result;
    }

    private JsonObject merge(String id) {

        JsonObject result = new JsonObject();

        if (id == null) {

            return result.put(ERROR, "Data is null");

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "select discoveryId,port,ip,name,password,type,community,version from Discovery AS D JOIN Credentials AS C ON D.credentialId = C.credentialId where discoveryId='" + id + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()) {

                for (int index = 1; index <= resultSet.getMetaData().getColumnCount(); index++) {

                    String columnName = resultSet.getMetaData().getColumnName(index);

                    if (!(resultSet.getString(columnName) == null)) {

                        result.put(columnName, resultSet.getObject(index));

                    }

                }

            }

        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

            result.put(ERROR, exception.getMessage());

        }

        return result;

    }

    private String validateProvision(JsonObject data) {

        if (data == null) {

            return "Data is null";

        }

        boolean isAvailable;

        if (!(data.containsKey(IP_ADDRESS) && data.containsKey(TYPE) && data.containsKey(CREDENTIAL_ID) && data.containsKey(TYPE) && data.containsKey(PORT))) {

            return FAIL;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String check = "select * from " + MONITOR + " where ip = '" + data.getString(IP_ADDRESS) + "' and type = '" + data.getString(TYPE) + "'";

            ResultSet set = connection.createStatement().executeQuery(check);

            boolean answer = set.next();

            if (!answer) {

                String query = "select result->>'$.status' from " + DISCOVERY_TABLE + " where credentialId = '" + data.getInteger(CREDENTIAL_ID) + "' and ip = '" + data.getString(IP_ADDRESS) + "' and type = '" + data.getString(TYPE) + "' ";

                ResultSet resultSet = connection.createStatement().executeQuery(query);

                isAvailable = resultSet.next();

                if (isAvailable) {

                    if (resultSet.getObject(1) != null) {

                        if (resultSet.getObject(1).toString().equalsIgnoreCase(SUCCESS)) {

                            return SUCCESS;

                        } else {

                            return NOT_DISCOVERED;

                        }

                    } else {

                        return NOT_DISCOVERED;

                    }

                } else {

                    return NOT_PRESENT;

                }

            } else {

                return EXIST;

            }

        } catch (Exception exception) {

            LOG.debug("Error : {}", exception.getMessage());

            return exception.getMessage();

        }

    }

    private JsonObject createContext(String id) {

        JsonObject result = new JsonObject();

        if (id == null) {

            return result.put(ERROR, "Data is null");

        }

        JsonArray metric;

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            metric = getAll(USER_METRIC, METRIC_ID, id);

            if (metric != null) {

                var user = metric.getJsonObject(0);

                String query = "select ip,type,port,name,password,community,version from Monitor,Credentials where monitorId = " + user.getString(MONITOR_ID) + " and credentialId = " + user.getString(CREDENTIAL_ID) + ";";

                ResultSet resultSet = connection.createStatement().executeQuery(query);

                while (resultSet.next()) {

                    result.put(IP_ADDRESS, resultSet.getObject(1));

                    result.put(TYPE, resultSet.getObject(2));

                    result.put(PORT, resultSet.getObject(3));

                    if (result.getString(TYPE).equalsIgnoreCase(LINUX) || result.getString(TYPE).equalsIgnoreCase(WINDOWS)) {

                        result.put(NAME, resultSet.getObject(4));

                        result.put(PASSWORD, resultSet.getObject(5));

                    } else if (result.getString(TYPE).equalsIgnoreCase(NETWORKING)) {

                        result.put(COMMUNITY, resultSet.getObject(6));

                        result.put(VERSION, resultSet.getObject(7));

                    }

                    result.put(METRIC_GROUP, user.getString(METRIC_GROUP));

                    result.put(TIME, user.getInteger(TIME));

                    result.put(METRIC_ID, user.getInteger(METRIC_ID));

                    result.put(MONITOR_ID, user.getInteger(MONITOR_ID));

                }

            } else {

                return new JsonObject().put(ERROR, NOT_PRESENT);

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

            return new JsonObject().put(ERROR, exception.getMessage());

        }

        return result;

    }

    private JsonArray pollingData(String column, String columnValue, String metricGroup, String limit) {

        if (column == null || columnValue == null || metricGroup == null) {

            return null;

        }

        JsonArray pollData = new JsonArray();

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query;

            if (metricGroup.equalsIgnoreCase(GETALL)) {

                ResultSet queryResult = connection.createStatement().executeQuery("select type from Monitor where monitorId = '" + columnValue + "'");

                queryResult.next();

                HashMap<String, Integer> counters = Utils.metric(queryResult.getString(1));

                for (Map.Entry<String, Integer> entry : counters.entrySet()) {

                    query = "select pollerId,result from Poller where " + column + "= '" + columnValue + "' and metricGroup = '" + entry.getKey() + "' ORDER BY pollerId DESC limit " + limit;

                    ResultSet resultSet = connection.createStatement().executeQuery(query);

                    while (resultSet.next()) {

                        JsonObject result = new JsonObject();

                        result.put(POLLING_ID, resultSet.getObject(1));

                        result.put(RESULT, resultSet.getObject(2));

                        pollData.add(result);

                    }
                }

            } else {

                query = "select pollerId,result from Poller where " + column + "= '" + columnValue + "' and metricGroup = '" + metricGroup + "' ORDER BY pollerId DESC limit " + limit;

                ResultSet resultSet = connection.createStatement().executeQuery(query);

                while (resultSet.next()) {

                    JsonObject result = new JsonObject();

                    result.put(POLLING_ID, resultSet.getObject(1));

                    result.put(RESULT, resultSet.getObject(2));

                    pollData.add(result);

                }

            }

//            query = "select pollerId,result from Poller where " + column + "= '" +columnValue + "' ORDER BY pollerId DESC limit " + limit;
//
//            ResultSet resultSet = connection.createStatement().executeQuery(query);
//
//            while (resultSet.next()) {
//
//                JsonObject result = new JsonObject();
//
//                result.put(POLLING_ID,resultSet.getObject(1));
//
//                result.put(RESULT,resultSet.getObject(2));
//
//                pollData.add(result);
//
//            }


        } catch (Exception exception) {

            LOG.debug("Error while fetching poll data {}", exception.getMessage());

            return null;

        }

        return pollData;


    }

    @Override
    public void start(Promise<Void> startPromise) {

        createTable();

        vertx.eventBus().<JsonObject>consumer(EVENTBUS_DATABASE, handler -> {

            switch (handler.body().getString(Constants.METHOD)) {

                case CREDENTIAL_POST_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject data = handler.body();

                            data.remove(METHOD);

                            if (containsAllCredential(data)) {

                                if (!check(CREDENTIAL_TABLE, CREDENTIAL_TABLE_NAME, data.getString(CREDENTIAL_NAME))) {

                                    blockingHandler.complete(data);

                                } else {

                                    blockingHandler.fail(CREDENTIAL_NAME_NOT_UNIQUE);

                                }

                            } else {

                                blockingHandler.fail(INVALID_INPUT);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());


                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case CREDENTIAL_DELETE_NAME_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            String id = handler.body().getString("id");

                            if (!check(USER_METRIC, CREDENTIAL_ID, id)) {

                                if (!check(Constants.DISCOVERY_TABLE, Constants.CREDENTIAL_ID, id)) {

                                    if (check(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, id)) {

                                        blockingHandler.complete();

                                    } else {

                                        blockingHandler.fail(Constants.NOT_PRESENT);

                                    }

                                } else {

                                    blockingHandler.fail(Constants.IN_USE);

                                }

                            } else {

                                blockingHandler.fail(IN_USE);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(Constants.FAIL);

                    }


                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        handler.reply(Constants.SUCCESS);

                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

                case DISCOVERY_POST_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject data = handler.body();

                            if (containsAllDiscovery(data)) {

                                if (check(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, data.getString(Constants.CREDENTIAL_ID))) {

                                    if (!check(Constants.DISCOVERY_TABLE, Constants.DISCOVERY_TABLE_NAME, data.getString(Constants.DISCOVERY_NAME))) {

                                        blockingHandler.complete(data);

                                    } else {

                                        blockingHandler.fail(Constants.DISCOVERY_NAME_NOT_UNIQUE);

                                    }

                                } else {

                                    blockingHandler.fail(Constants.INVALID_CREDENTIAL_ID);

                                }

                            } else {

                                blockingHandler.fail(INVALID_INPUT);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(Constants.FAIL);

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());


                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case DATABASE_ID_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            if (check(userData.getString(TABLE_NAME), userData.getString(TABLE_COLUMN), userData.getString(TABLE_ID))) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(Constants.NOT_PRESENT);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(Constants.FAIL);

                    }

                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        handler.reply(Constants.SUCCESS);

                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

                case DATABASE_INSERT -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            userData.remove(METHOD);

                            JsonObject result = insert(userData.getString(TABLE_NAME), userData);

                            if (result == null) {

                                blockingHandler.fail(Constants.INVALID_INPUT);

                            } else if (result.containsKey(ERROR)) {

                                blockingHandler.fail(result.getString(ERROR));

                            } else if (result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)) {

                                blockingHandler.complete(userData);

                            } else {

                                blockingHandler.fail(Constants.FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }


                    } catch (Exception exception) {

                        LOG.debug("Error : {}" + exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completeHandler -> {

                    if (completeHandler.succeeded()) {

                        handler.reply(completeHandler.result());

                    } else {

                        handler.fail(-1, completeHandler.cause().getMessage());

                    }

                });

                case DATABASE_GET -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonArray result = getAll(handler.body().getString(TABLE_NAME), handler.body().getString(TABLE_COLUMN), handler.body().getString(TABLE_ID));

                            if (result != null) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(FAIL);

                            }
                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(Constants.FAIL);

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case DATABASE_DELETE -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            if (delete(handler.body().getString(TABLE_NAME), handler.body().getString(TABLE_COLUMN), handler.body().getString(TABLE_ID))) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error : {}" + exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completeHandler -> {

                    if (completeHandler.succeeded()) {

                        handler.reply(Constants.SUCCESS);
                    } else {

                        handler.fail(-1, completeHandler.cause().getMessage());

                    }
                });

                case DATABASE_UPDATE -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            userData.remove(METHOD);

                            if (update(userData.getString(TABLE_NAME), userData.getString(TABLE_COLUMN), userData)) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(Constants.FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        handler.reply(Constants.SUCCESS);

                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

                case MERGE_DATA -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject result = merge(handler.body().getString("id"));

                            if (!result.containsKey(ERROR)) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(result.getString(ERROR));

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error Merge data {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.reply(completionHandler.cause().getMessage());

                    }

                });

                case RUN_DISCOVERY_INSERT -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            userData.remove(METHOD);

                            if (update(DISCOVERY_TABLE, DISCOVERY_TABLE_ID, userData)) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(SUCCESS);


                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case VALIDATE_PROVISION -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject data = handler.body();

                            data.remove(METHOD);

                            if (containsAllMonitor(data)) {

                                String result = validateProvision(data);

                                if (result != null) {

                                    if (result.equalsIgnoreCase(SUCCESS)) {

                                        blockingHandler.complete(data);

                                    } else {

                                        blockingHandler.fail(result);

                                    }

                                } else {

                                    blockingHandler.fail(INVALID_INPUT);

                                }

                            } else {

                                blockingHandler.fail(OBJECT_MISSING);

                            }

                        } else {

                            blockingHandler.fail(INVALID_INPUT);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case INSERT_METRIC -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            userData.remove(METHOD);

                            JsonObject result;

                            JsonArray metric = new JsonArray();

                            String type = null;

                            if (userData.containsKey(TYPE)) {

                                type = userData.getString(TYPE);

                                userData.remove(TYPE);

                            }

                            if (type != null) {

                                HashMap<String, Integer> map = Utils.metric(type);

                                String objects = null;

                                if (userData.containsKey(OBJECTS)) {

                                    objects = userData.getString(OBJECTS);

                                    userData.remove(OBJECTS);

                                }

                                for (Map.Entry<String, Integer> entry : map.entrySet()) {

                                    userData.put(METRIC_GROUP, entry.getKey());

                                    userData.put(TIME, entry.getValue());

                                    if (getId(USER_METRIC, METRIC_ID) != -1) {

                                        userData.put(METRIC_ID, getId(USER_METRIC, METRIC_ID));
                                    }

                                    if (entry.getKey().equalsIgnoreCase("interface")) {

                                        if (objects != null) {

                                            userData.put(OBJECTS, objects);
                                        }

                                    }

                                    if (containsAllUserMetric(userData)) {

                                        result = insert(USER_METRIC, userData);

                                        if (result != null) {

                                            if (result.containsKey(STATUS)) {

                                                if (result.getString(STATUS).equalsIgnoreCase(SUCCESS)) {

                                                    JsonObject user = userData.copy();

                                                    metric.add(user);

                                                }

                                            } else if (result.containsKey(ERROR)) {

                                                blockingHandler.fail(result.getString(ERROR));

                                            }

                                        } else {

                                            blockingHandler.fail(INVALID_INPUT);

                                        }

                                    } else {

                                        blockingHandler.fail(INVALID_INPUT);

                                    }

                                }

                                if (metric.size() == map.size()) {

                                    blockingHandler.complete(metric);

                                } else {

                                    blockingHandler.fail(FAIL);

                                }
                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(INVALID_INPUT);

                        }


                    } catch (Exception exception) {

                        LOG.debug("Error : {}" + exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completeHandler -> {

                    if (completeHandler.succeeded()) {

                        handler.reply(completeHandler.result());

                    } else {

                        handler.fail(-1, completeHandler.cause().getMessage());

                    }

                });


                case DATABASE_DELETE_MONITOR -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonArray metricId = getAll(USER_METRIC, MONITOR_ID, handler.body().getString(TABLE_ID));

                        if (delete(USER_METRIC, MONITOR_ID, handler.body().getString(TABLE_ID))) {

                            if (delete(MONITOR, MONITOR_ID, handler.body().getString(TABLE_ID))) {

                                blockingHandler.complete(metricId);

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(Constants.FAIL);

                        }


                    } catch (Exception exception) {

                        LOG.debug("Error : Database delete monitor {}" + exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completeHandler -> {

                    if (completeHandler.succeeded()) {

                        vertx.eventBus().send(SCHEDULER_DELETE, completeHandler.result());

                        handler.reply(Constants.SUCCESS);


                    } else {

                        handler.fail(-1, completeHandler.cause().getMessage());

                    }
                });

                case CREATE_CONTEXT -> vertx.<JsonObject>executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject result = createContext(handler.body().getString(METRIC_ID));

                            if (result != null) {

                                if (!result.containsKey(ERROR)) {

                                    blockingHandler.complete(result);

                                } else {

                                    blockingHandler.fail(result.getString(ERROR));

                                }

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error: create context {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case DATABASE_GET_POLL_DATA -> vertx.<JsonArray>executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonArray result = pollingData(handler.body().getString(TABLE_COLUMN), handler.body().getString(TABLE_ID), handler.body().getString(METRIC_GROUP), handler.body().getString(LIMIT));

                            if (result != null) {

                                blockingHandler.complete(result);

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error: create context {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case DATABASE_UPDATE_GROUP_TIME -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (handler.body() != null) {

                            JsonObject userData = handler.body();

                            userData.remove(METHOD);

                            if (update(USER_METRIC, MONITOR_ID, userData)) {

                                JsonArray data = getAll(USER_METRIC, MONITOR_ID, userData.getString(MONITOR_ID));

                                for (int index = 0; index < data.size(); index++) {

                                    var json = data.getJsonObject(index);

                                    if (json.getString(METRIC_GROUP).equalsIgnoreCase(userData.getString(METRIC_GROUP))) {

                                        userData.put(METRIC_ID, json.getInteger(METRIC_ID));

                                        blockingHandler.complete(userData);

                                        break;

                                    }

                                }

                            } else {

                                blockingHandler.fail(FAIL);

                            }


                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {}", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        vertx.eventBus().send(SCHEDULER_UPDATE, resultHandler.result());

                        handler.reply(Constants.SUCCESS);

                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

            }

        });

        startPromise.complete();

    }
}
