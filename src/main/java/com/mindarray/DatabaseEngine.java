package com.mindarray;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.UUID;

import static com.mindarray.Constants.*;

public class DatabaseEngine extends AbstractVerticle {

    static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    boolean checkName(String table, String column, String value) {

        boolean isAvailable = false;

        if (table == null || column == null || value == null) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query = "select * from " + table + " where " + column + "='" + value + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            isAvailable = resultSet.next();

        } catch (Exception exception) {

            LOG.debug("Error : {}", exception.getMessage());

        }

        return isAvailable;

    }

    boolean containsAllCredential(JsonObject data) {

        data.put(CREDENTIAL_ID, UUID.randomUUID().toString());

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

    boolean containsAllDiscovery(JsonObject data) {

        data.put(DISCOVERY_TABLE_ID, UUID.randomUUID().toString());

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

    boolean containsAllMonitor(JsonObject data){

        data.put(MONITOR_ID,UUID.randomUUID().toString());

        data.remove(CREDENTIAL_ID);

        data.remove(OBJECTS);

        if(data.containsKey(MONITOR_ID) && data.containsKey(IP_ADDRESS) && data.containsKey(PORT) && data.containsKey(TYPE)){

            return true;

        }else{

            return false;

        }

    }


    void createTable() {

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            Statement stmt = connection.createStatement();

            stmt.executeUpdate("create table if not exists Credentials (credentialId varchar(255),credential_name varchar(255) PRIMARY KEY,protocol varchar(255),name varchar(255),password varchar(255),community varchar(255),version varchar(255))");

            stmt.executeUpdate("create table if not exists Discovery (discoveryId varchar(255),credentialId varchar(255),discovery_name varchar(255) PRIMARY KEY,ip varchar(255),type varchar(255),port int,result JSON)");

            stmt.executeUpdate("create table if not exists Monitor (monitorId varchar(255),ip varchar(255),type varchar(255),port int,objects JSON)");

            stmt.executeUpdate("create table if not exists UserMetric (metricId varchar(255),monitorId varchar(255),credentialId varchar(255),metricGroup varchar(255),time int,objects JSON)");


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }
    }

    JsonObject insert(String tableName, JsonObject userData) {

        JsonObject result = new JsonObject();

        userData.remove(TABLE_NAME);

        if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE) && !containsAllCredential(userData)) {

            return null;

        } else if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && !containsAllDiscovery(userData)) {

            return null;

        } else if(tableName.equalsIgnoreCase(MONITOR) && !containsAllMonitor(userData)){

            return null;
        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE) && checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_TABLE_NAME, userData.getString(Constants.CREDENTIAL_NAME))) {

                result.put(Constants.STATUS, Constants.FAIL);

                return result;

            }


            if (tableName.equalsIgnoreCase(DISCOVERY_TABLE)) {

                if (checkName(Constants.DISCOVERY_TABLE, Constants.DISCOVERY_TABLE_NAME, userData.getString(Constants.DISCOVERY_NAME))) {

                    result.put(Constants.STATUS, Constants.FAIL);

                    return result;

                }

                if (!checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, userData.getString(Constants.CREDENTIAL_ID))) {

                    result.put(Constants.STATUS, Constants.FAIL);

                    return result;

                }

            }



            StringBuilder column = new StringBuilder("insert into ").append(tableName).append("(");

            StringBuilder values = new StringBuilder(" values (");

            // insert into tableName (  c1,c2,c3....)
            // values ( v1,v2,v3....);

            Map<String, Object> userMap = userData.getMap();

//            Map<String,String> dataMap = transform();

            for (Map.Entry<String, Object> entry : userMap.entrySet()) {

                column.append(entry.getKey().replace(".", "_")).append(",");

                if (entry.getValue() instanceof String) {

                    values.append("'").append(entry.getValue()).append("'").append(",");

                } else {

                    values.append(entry.getValue()).append(",");

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

            result.put(Constants.STATUS, Constants.FAIL);

        }

        return result;

    }

    boolean update(String tableName, String columnName, JsonObject userData) {

        boolean result = true;

        String id = userData.getString(columnName);

        if (tableName.equalsIgnoreCase(CREDENTIAL_TABLE) && !userData.containsKey(Constants.CREDENTIAL_ID)) {

            return false;

        }

        if (tableName.equalsIgnoreCase(DISCOVERY_TABLE) && !userData.containsKey(Constants.DISCOVERY_TABLE_ID)) {

            return false;

        }

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

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

            // UPDATE Credentials SET column1 = 'value1', column2 = 'value2' ..... where columnValue = idValue;

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

    JsonArray getAll(String tableName, String column, String id) {

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

                    if(!(resultSet.getString(columnName) == null)) {

                        result.put(newColumnName, resultSet.getObject(i));

                    }

//                    if (columnName.equalsIgnoreCase(PROTOCOL)) {
//
//                        if (resultSet.getString(i).equalsIgnoreCase(SSH) || resultSet.getString(i).equalsIgnoreCase(WINRM)) {
//
//                            columnCount = columnCount - 2;
//
//                        } else {
//
//                            i = i + 2;
//
//                        }
//                    }
                }

                jsonArray.add(result);
            }


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            System.out.println("ex " + exception.getMessage());

        }

        return jsonArray;

    }

    boolean delete(String tableName, String column, String id) {

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

        }

        return result;
    }

    JsonObject merge(String id){

        JsonObject result = new JsonObject();

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String query =  "select discoveryId,port,ip,name,password,type,community,version from Discovery AS D JOIN Credentials AS C ON D.credentialId = C.credentialId where discoveryId='" + id + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()){

                for(int i=1;i<=resultSet.getMetaData().getColumnCount();i++){

                    String columnName = resultSet.getMetaData().getColumnName(i);

                    if(!(resultSet.getString(columnName) == null)) {

                        result.put(columnName, resultSet.getObject(i));

                    }

                }

            }

        }catch (Exception exception){

            LOG.debug("Error : {} ",exception.getMessage());

        }

        return result;

    }

    String validateProvision(JsonObject data){

        String ip = data.getString(IP_ADDRESS);

        String type = data.getString(TYPE);

        String credId = data.getString(CREDENTIAL_ID);


        boolean isAvailable;

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password")) {

            String check = "select * from " + MONITOR + " where ip = '" + ip + "' and type = '" + type + "'";

            ResultSet set = connection.createStatement().executeQuery(check);

            boolean ans = set.next();

            if(!ans) {

                String query = "select result->>'$.status' from " + DISCOVERY_TABLE + " where credentialId = '" + credId + "' and ip = '" + ip + "' and type = '" + type + "'";

                ResultSet resultSet = connection.createStatement().executeQuery(query);

                isAvailable = resultSet.next();

                if (isAvailable) {

                    if (resultSet.getObject(1) != null) {

                        if (resultSet.getObject(1).toString().equalsIgnoreCase(SUCCESS)) {

                            return SUCCESS;

                        } else {

                            return FAIL;

                        }

                    } else {

                        return NOT_DISCOVERED;

                    }

                } else {

                    return NOT_PRESENT;

                }

            }else{

                return EXIST;

            }

        } catch (Exception exception) {

            LOG.debug("Error : {}", exception.getMessage());

        }

        return FAIL;

    }


    @Override
    public void start(Promise<Void> startPromise) {

        createTable();

        vertx.eventBus().<JsonObject>consumer(EVENTBUS_DATABASE, handler -> {

            switch (handler.body().getString(Constants.METHOD)) {

                case CREDENTIAL_POST_CHECK_NAME -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonObject data = handler.body();

                        data.remove(METHOD);

                        if (!checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_TABLE_NAME, data.getString(Constants.CREDENTIAL_NAME))) {

                            blockingHandler.complete(data);

                        } else {

                            blockingHandler.fail(CREDENTIAL_NAME_NOT_UNIQUE);

                        }

                    } catch (Exception exception) {

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

                case DISCOVERY_POST_CHECK_NAME -> vertx.executeBlocking(blockingHandler -> {

                    try {

                        JsonObject data = handler.body();

                        if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, data.getString(Constants.CREDENTIAL_ID))) {

                            if (!checkName(Constants.DISCOVERY_TABLE, Constants.DISCOVERY_TABLE_NAME, data.getString(Constants.DISCOVERY_NAME))) {

                                blockingHandler.complete(data);

                            } else {

                                blockingHandler.fail(Constants.DISCOVERY_NAME_NOT_UNIQUE);

                            }

                        } else {

                            blockingHandler.fail(Constants.INVALID_CREDENTIAL_ID);

                        }

                    } catch (Exception exception) {

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

                    vertx.executeBlocking(request -> {

                        JsonObject result;

                        try {

                            result = insert(userData.getString(TABLE_NAME), userData);

                            if (result == null) {

                                request.fail(Constants.INVALID_INPUT);

                            } else if (result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)) {

                                request.complete();

                            } else {

                                request.fail(Constants.FAIL);

                            }


                        } catch (Exception exception) {

                            LOG.debug("Error : {}" + exception.getMessage());

                            request.fail(exception.getMessage());

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

                    boolean result;

                    try {

                        result = update(userData.getString(TABLE_NAME), userData.getString(TABLE_COLUMN), userData);

                        if (result) {

                            blockingHandler.complete();
                        } else {

                            blockingHandler.fail(Constants.FAIL);

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

                case MERGE_DATA -> vertx.executeBlocking(blockingHandler -> {

                    JsonObject data = merge(handler.body().getString("id"));

                    blockingHandler.complete(data);

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

                    if (update(DISCOVERY_TABLE, DISCOVERY_TABLE_ID, userData)) {

                        blockingHandler.complete();

                    } else {

                        blockingHandler.fail(FAIL);

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

                    String result = validateProvision(data);

                    if (result.equalsIgnoreCase(SUCCESS)) {

                        blockingHandler.complete(SUCCESS);

                    } else {

                        blockingHandler.fail(result);

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

                    userData.remove(TABLE_COLUMN);

                    vertx.executeBlocking(request -> {

                        JsonObject result;

                        try {

                            result = insert(userData.getString(TABLE_NAME), userData);

                            if (result == null) {

                                request.fail(Constants.INVALID_INPUT);

                            } else if (result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)) {

                                request.complete();

                            } else {

                                request.fail(Constants.FAIL);

                            }


                        } catch (Exception exception) {

                            LOG.debug("Error : {}" + exception.getMessage());

                            request.fail(exception.getMessage());

                        }

                    }).onComplete(completeHandler -> {

                        if (completeHandler.succeeded()) {

                            handler.reply(userData);
                        } else {

                            handler.fail(-1, completeHandler.cause().getMessage());

                        }

                    });

                }
            }




        });

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------

        startPromise.complete();

    }
}
