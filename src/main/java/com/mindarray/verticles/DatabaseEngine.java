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

    private boolean checkName(String table, String column, String value) {

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

        data.put(CREDENTIAL_ID, getId(CREDENTIAL_TABLE, CREDENTIAL_ID));

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

            if (!(data.containsKey(Constants.PASSWORD) && (!data.getString(Constants.PASSWORD).isEmpty()))) {

                return false;

            }
        }

        if (data.getString(Constants.PROTOCOL).equalsIgnoreCase(SNMP)) {

            if (!(data.containsKey(Constants.COMMUNITY) && (!data.getString(Constants.COMMUNITY).isEmpty()))) {

                return false;

            }

            if (!(data.containsKey(Constants.VERSION) && (!data.getString(Constants.VERSION).isEmpty()))) {

                return false;

            }
        }

        return true;

    }

    private boolean containsAllDiscovery(JsonObject data) {

        data.put(DISCOVERY_TABLE_ID, getId(DISCOVERY_TABLE, DISCOVERY_TABLE_ID));

        if (!(data.containsKey(Constants.CREDENTIAL_ID) && (!data.getString(Constants.CREDENTIAL_ID).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.DISCOVERY_TABLE_ID) && (!data.getString(Constants.DISCOVERY_TABLE_ID).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.PORT) && (!data.getString(Constants.PORT).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.IP_ADDRESS) && (!data.getString(Constants.IP_ADDRESS).isEmpty()))) {

            return false;

        }
        if (!(data.containsKey(Constants.TYPE) && (!data.getString(Constants.TYPE).isEmpty()))) {

            return false;

        }

        return true;

    }

    private boolean containsAllMonitor(JsonObject data) {

        data.put(MONITOR_ID, getId(MONITOR, MONITOR_ID));

        return data.containsKey(MONITOR_ID) && data.containsKey(IP_ADDRESS) && data.containsKey(PORT) && data.containsKey(TYPE);

    }

    private boolean containsAllUserMetric(JsonObject data) {

        return data.containsKey(CREDENTIAL_ID) && data.containsKey(MONITOR_ID) && data.containsKey(METRIC_ID) && data.containsKey(METRIC_GROUP) && data.containsKey(TIME);

    }

    private void createTable() {

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            Statement statement = connection.createStatement();

            statement.executeUpdate("create table if not exists Credentials (credentialId int PRIMARY KEY AUTO_INCREMENT,credential_name varchar(255),protocol varchar(255),name varchar(255),password varchar(255),community varchar(255),version varchar(255))");

            statement.executeUpdate("create table if not exists Discovery (discoveryId int PRIMARY KEY AUTO_INCREMENT,credentialId int,discovery_name varchar(255),ip varchar(255),type varchar(255),port int,result JSON)");

            statement.executeUpdate("create table if not exists Monitor (monitorId int PRIMARY KEY AUTO_INCREMENT,ip varchar(255),type varchar(255),port int,host varchar(255))");

            statement.executeUpdate("create table if not exists UserMetric (metricId int PRIMARY KEY AUTO_INCREMENT,monitorId int,credentialId int,metricGroup varchar(255),time int,objects JSON)");


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }
    }

    private JsonObject insert(String tableName, JsonObject userData) {

        JsonObject result = new JsonObject();

        userData.remove(TABLE_NAME);

        if (tableName.equalsIgnoreCase(USER_METRIC) && !containsAllUserMetric(userData)) {

            return null;

        }

        if (!tableName.equalsIgnoreCase(USER_METRIC)){

            userData.remove(OBJECTS);

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

            return new JsonObject().put(ERROR,exception.getMessage());

        }

        return result;

    }

    private boolean update(String tableName, String columnName, JsonObject userData) {

        boolean result = true;

        String id = userData.getString(columnName);

        if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE) && !userData.containsKey(Constants.CREDENTIAL_ID)) {

            return false;

        }

        if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && !userData.containsKey(Constants.DISCOVERY_TABLE_ID)) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            // shift validation

            if (userData.containsKey(Constants.CREDENTIAL_NAME)) {

                if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_TABLE_NAME, userData.getString(Constants.CREDENTIAL_NAME))) {

                    return false;

                }

                if (!(userData.containsKey(Constants.IP_ADDRESS) || userData.containsKey(Constants.DISCOVERY_NAME) || userData.containsKey(Constants.CREDENTIAL_ID))) {

                    return false;

                }

            } else if (userData.containsKey(Constants.DISCOVERY_NAME)) {

                if (checkName(Constants.DISCOVERY_TABLE, Constants.DISCOVERY_TABLE_NAME, userData.getString(Constants.DISCOVERY_NAME))) {

                    return false;

                }
            }

            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && userData.containsKey(CREDENTIAL_ID) && !checkName(CREDENTIAL_TABLE, CREDENTIAL_ID, userData.getString(CREDENTIAL_ID))) {

                return false;

            }


            Map<String, Object> data = userData.getMap();

            if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE)) {

                data.remove(CREDENTIAL_ID);

                data.remove(PROTOCOL);

            }

            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE)) {

                data.remove(TYPE);

                data.remove(PORT);

                data.remove(DISCOVERY_TABLE_ID);

            }

            data.remove(TABLE_COLUMN, columnName);

            data.remove(TABLE_NAME);

            data.remove(TABLE_ID);

            String query;

            StringBuilder update = new StringBuilder();

            query = "UPDATE " + tableName + " SET ";

            for (Map.Entry<String, Object> entry : data.entrySet()) {

                update.append(entry.getKey().replace(".", "_")).append(" = ").append("'").append(entry.getValue()).append("'").append(",");

            }

            update.deleteCharAt(update.length() - 1);

            String updatedQuery = query + update + " where " + columnName + " = '" + id + "' ;";

            PreparedStatement preparedStatement = connection.prepareStatement(updatedQuery);

            preparedStatement.execute();


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return false;

        }

        return result;

    }

    private JsonArray getAll(String tableName, String column, String id) {

        JsonArray jsonArray = null;

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            jsonArray = new JsonArray();

            String query;

            if (id.equalsIgnoreCase("getall")) {

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

                jsonArray.add(result);

            }


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            // return error

        }

        return jsonArray;

    }

    private boolean delete(String tableName, String column, String id) {

        boolean result = false;

        if (id == null || tableName == null || column == null) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "delete from " + tableName + " where " + column + " ='" + id + "'";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            int a = preparedStatement.executeUpdate();

            if (a > 0) {

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

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "select discoveryId,port,ip,name,password,type,community,version from Discovery AS D JOIN Credentials AS C ON D.credentialId = C.credentialId where discoveryId='" + id + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()) {

                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {

                    String columnName = resultSet.getMetaData().getColumnName(i);

                    if (!(resultSet.getString(columnName) == null)) {

                        result.put(columnName, resultSet.getObject(i));

                    }

                }

            }

        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }

        return result;

    }

    private String validateProvision(JsonObject data) {

        boolean isAvailable;

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String check = "select * from " + MONITOR + " where ip = '" + data.getString(IP_ADDRESS) + "' and type = '" + data.getString(TYPE) + "'";

            ResultSet set = connection.createStatement().executeQuery(check);

            boolean ans = set.next();

            if (!ans) {

                String query = "select result->>'$.status' from " + DISCOVERY_TABLE + " where credentialId = '" + data.getString(CREDENTIAL_ID) + "' and ip = '" + data.getString(IP_ADDRESS) + "' and type = '" + data.getString(TYPE) + "' and port = " + data.getInteger(PORT) + "";

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

        JsonArray metric;

        JsonObject result = new JsonObject();

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            metric = getAll(USER_METRIC, METRIC_ID, id);

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

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

            return new JsonObject().put(ERROR, exception.getMessage());

        }

        return result;

    }

    @Override
    public void start(Promise<Void> startPromise) {

        createTable();

        vertx.eventBus().<JsonObject>consumer(EVENTBUS_DATABASE, handler -> {

            switch (handler.body().getString(Constants.METHOD)) {

                case CREDENTIAL_POST_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonObject data = handler.body();

                        data.remove(METHOD);

                        if (containsAllCredential(data)) {

                            if (!checkName(CREDENTIAL_TABLE, CREDENTIAL_TABLE_NAME, data.getString(CREDENTIAL_NAME))) {

                                blockingHandler.complete(data);

                            } else {

                                blockingHandler.fail(CREDENTIAL_NAME_NOT_UNIQUE);

                            }

                        } else {

                            blockingHandler.fail(INVALID_INPUT);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(exception.getMessage());

                    }

                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        handler.reply(resultHandler.result());


                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

                case CREDENTIAL_DELETE_NAME_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    String id = handler.body().getString("id");

                    try {

                        if (!checkName(Constants.DISCOVERY_TABLE, Constants.CREDENTIAL_ID, id)) {

                            if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, id)) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(Constants.NOT_PRESENT);

                            }

                        } else {

                            blockingHandler.fail(Constants.IN_USE);

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

                        JsonObject data = handler.body();

                        if (containsAllDiscovery(data)) {

                            if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, data.getString(Constants.CREDENTIAL_ID))) {

                                if (!checkName(Constants.DISCOVERY_TABLE, Constants.DISCOVERY_TABLE_NAME, data.getString(Constants.DISCOVERY_NAME))) {

                                    blockingHandler.complete(data);

                                } else {

                                    blockingHandler.fail(Constants.DISCOVERY_NAME_NOT_UNIQUE);

                                }

                            } else {

                                blockingHandler.fail(Constants.INVALID_CREDENTIAL_ID);

                            }

                        }else {

                            blockingHandler.fail(INVALID_INPUT);

                        }

                    } catch (Exception exception) {

                        LOG.debug("Error {} ", exception.getMessage());

                        blockingHandler.fail(Constants.FAIL);

                    }


                }).onComplete(resultHandler -> {

                    if (resultHandler.succeeded()) {

                        handler.reply(resultHandler.result());


                    } else {

                        handler.fail(-1, resultHandler.cause().getMessage());

                    }

                });

                case DATABASE_ID_CHECK -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonObject userData = handler.body();

                        if (checkName(userData.getString(TABLE_NAME), userData.getString(TABLE_COLUMN), userData.getString(TABLE_ID))) {

                            blockingHandler.complete();

                        } else {

                            blockingHandler.fail(Constants.NOT_PRESENT);

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

                case DATABASE_INSERT -> {

                    JsonObject userData = handler.body();

                    userData.remove(METHOD);

                    System.out.println("insert" + userData);

                    vertx.executeBlocking(blockingHandler -> {

                        JsonObject result;

                        try {

                            result = insert(userData.getString(TABLE_NAME), userData);

                            if (result == null) {

                                blockingHandler.fail(Constants.INVALID_INPUT);

                            } else if (result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(Constants.FAIL);

                            }


                        } catch (Exception exception) {

                            LOG.debug("Error : {}" + exception.getMessage());

                            blockingHandler.fail(exception.getMessage());

                        }

                    }).onComplete(completeHandler -> {

                        if (completeHandler.succeeded()) {

                            handler.reply(userData);

                        } else {

                            handler.fail(-1, completeHandler.cause().getMessage());

                        }

                    });

                }

                case DATABASE_GET -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonArray result = getAll(handler.body().getString(TABLE_NAME), handler.body().getString(TABLE_COLUMN), handler.body().getString(TABLE_ID));

                        if (result != null) {

                            blockingHandler.complete(result);

                        } else {

                            blockingHandler.fail(Constants.FAIL);

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

                    boolean result;

                    try {

                        result = delete(handler.body().getString(TABLE_NAME), handler.body().getString(TABLE_COLUMN), handler.body().getString(TABLE_ID));

                        if (result) {

                            blockingHandler.complete();

                        } else {

                            blockingHandler.fail(Constants.FAIL);

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

                    JsonObject userData = handler.body();

                    userData.remove(METHOD);

                    try {

                        if (update(userData.getString(TABLE_NAME), userData.getString(TABLE_COLUMN), userData)) {

                            blockingHandler.complete();

                        } else {

                            blockingHandler.fail(Constants.FAIL);

                        }

                    }catch (Exception exception){

                        LOG.debug("Error {}",exception.getMessage());

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

                    if (handler.body() != null) {

                        blockingHandler.complete(merge(handler.body().getString("id")));

                    } else {

                        blockingHandler.fail(FAIL);

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.reply(completionHandler.cause().getMessage());

                    }

                });

                case RUN_DISCOVERY_INSERT -> vertx.executeBlocking(blockingHandler -> {

                    JsonObject userData = handler.body();

                    userData.remove(METHOD);

                    try {

                        if (update(DISCOVERY_TABLE, DISCOVERY_TABLE_ID, userData)) {

                            blockingHandler.complete();

                        } else {

                            blockingHandler.fail(FAIL);

                        }

                    }catch (Exception exception){

                        LOG.debug("Error {}" ,exception.getMessage());

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

                    JsonObject data = handler.body();

                    data.remove(METHOD);

                    if (data.containsKey(TYPE)) {

                        if (data.getString(TYPE).equalsIgnoreCase(NETWORKING)) {

                            if (!data.containsKey(OBJECTS)) {

//                                handler.fail(-1, OBJECT_MISSING);  // check once
                                blockingHandler.fail(OBJECT_MISSING);

                            }
                        }
                    }

                    if (containsAllMonitor(data)){

                        String result = validateProvision(data);

                        if (result.equalsIgnoreCase(SUCCESS)) {

                            blockingHandler.complete(data);

                        } else {

                            blockingHandler.fail(result);

                        }

                    }else {

                        blockingHandler.fail(INVALID_INPUT);

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, completionHandler.cause().getMessage());

                    }

                });

                case INSERT_METRIC -> {

                    JsonObject userData = handler.body();

                    userData.remove(METHOD);

                    userData.remove(HOST);

                    userData.remove(IP_ADDRESS);

                    userData.remove(PORT);

                    vertx.executeBlocking(blockingHandler -> {

                        JsonObject result;

                        JsonArray metric = new JsonArray();

                        String type = null;

                        if(userData.containsKey(TYPE)) {

                            type = userData.getString(TYPE);

                            userData.remove(TYPE);

                        }

                        try {

                            if (type!=null) {

                                HashMap<String, Integer> map = Utils.metric(type);

                                String objects = null;

                                if (userData.containsKey(OBJECTS)) {

                                    objects = userData.getString(OBJECTS);

                                    userData.remove(OBJECTS);

                                }

                                for (Map.Entry<String, Integer> entry : map.entrySet()) {

                                    userData.put(METRIC_GROUP, entry.getKey());

                                    userData.put(TIME, entry.getValue());

                                    userData.put(METRIC_ID, getId(USER_METRIC, METRIC_ID));

                                    if (entry.getKey().equalsIgnoreCase("interface")) {

                                        userData.put(OBJECTS, objects);

                                    }

                                    if (containsAllUserMetric(userData)) {

                                        result = insert(USER_METRIC, userData);

                                        if (result!=null) {

                                            if (result.containsKey(STATUS)) {

                                                if (result.getString(STATUS).equalsIgnoreCase(SUCCESS)) {

                                                    JsonObject user = userData.copy();

                                                    metric.add(user);

                                                }

                                            }
                                        }else {

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

                            }else {

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

                }

                case DATABASE_DELETE_MONITOR -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        if (delete(USER_METRIC, MONITOR_ID, handler.body().getString(TABLE_ID))) {

                            if (delete(MONITOR, MONITOR_ID, handler.body().getString(TABLE_ID))) {

                                blockingHandler.complete();

                            } else {

                                blockingHandler.fail(FAIL);

                            }

                        } else {

                            blockingHandler.fail(Constants.FAIL);

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

                case CREATE_CONTEXT -> vertx.<JsonObject>executeBlocking(blockingHandler -> {

                    JsonObject result = createContext(handler.body().getString(METRIC_ID));

                    if (result != null) {

                        blockingHandler.complete(result);

                    } else {

                        blockingHandler.fail(FAIL);

                    }

                }).onComplete(completionHandler -> {

                    if (completionHandler.succeeded()) {

                        handler.reply(completionHandler.result());

                    } else {

                        handler.fail(-1, "Error in Creating context");

                    }

                });

            }

        });


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------

        startPromise.complete();

    }
}
