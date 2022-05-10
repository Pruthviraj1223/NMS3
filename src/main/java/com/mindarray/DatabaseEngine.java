package com.mindarray;

import io.vertx.core.AbstractVerticle;

import io.vertx.core.Promise;

import io.vertx.core.json.JsonArray;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.sql.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class DatabaseEngine extends AbstractVerticle {

    boolean checkName(String table,String column,String value) throws SQLException {

        Connection connection = null;

        boolean isAvailable = false;

        if (table==null || column==null || value==null) {

            return false;

        }


        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "select * from " +  table  + " where " + column  + "='" + value + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            isAvailable = resultSet.next();

        } catch (Exception exception) {

            LOG.debug("Error : {}", exception.getMessage());

        } finally {

            if (connection != null) {

                connection.close();

            }

        }

        return isAvailable;

    }

    boolean containsAllCredential(JsonObject data) {

        if (!(data.containsKey(Constants.CREDENTIAL_ID) && (!data.getString(Constants.CREDENTIAL_ID).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.CREDENTIAL_NAME) && (!data.getString(Constants.CREDENTIAL_NAME).isEmpty()))) {

            return false;

        }

        if (!(data.containsKey(Constants.PROTOCOL) && (!data.getString(Constants.PROTOCOL).isEmpty()))) {

            return false;

        }


        if (data.getString(Constants.PROTOCOL).equalsIgnoreCase("ssh") || data.getString(Constants.PROTOCOL).equalsIgnoreCase("winrm")) {

            if (!(data.containsKey(Constants.NAME) && (!data.getString(Constants.NAME).isEmpty()))) {

                return false;

            }

            if (!(data.containsKey(Constants.PASSWORD) && (!data.getString(Constants.PASSWORD).isEmpty()))) {

                return false;

            }
        }

        if (data.getString(Constants.PROTOCOL).equalsIgnoreCase("snmp")) {

            if (!(data.containsKey(Constants.COMMUNITY) && (!data.getString(Constants.COMMUNITY).isEmpty()))) {

                return false;

            }

            if (!(data.containsKey(Constants.VERSION) && (!data.getString(Constants.VERSION).isEmpty()))) {

                return false;

            }
        }

        return true;

    }

    boolean containsAllDiscovery(JsonObject data){

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


    JsonObject insertCredentials(JsonObject userData) throws SQLException {

        Connection connection = null;

        JsonObject result = new JsonObject();

        if (!containsAllCredential(userData)) {

            return null;

        }

        try {

            if (checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_TABLE_NAME,userData.getString(Constants.CREDENTIAL_NAME))) {

                result.put(Constants.STATUS, Constants.FAIL);

                return result;

            }

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            PreparedStatement preparedStatement = connection.prepareStatement("insert into Credentials (credentialId,credentialName,protocol,name,password,community,version) values (?,?,?,?,?,?,?)");

            preparedStatement.setString(1, userData.getString(Constants.CREDENTIAL_ID));

            preparedStatement.setString(2, userData.getString(Constants.CREDENTIAL_NAME));

            preparedStatement.setString(3, userData.getString(Constants.PROTOCOL));

            preparedStatement.setString(4, userData.getString(Constants.NAME));

            preparedStatement.setString(5, userData.getString(Constants.PASSWORD));

            preparedStatement.setString(6, userData.getString(Constants.COMMUNITY));

            preparedStatement.setString(7, userData.getString(Constants.VERSION));

            preparedStatement.executeUpdate();

            result.put(Constants.STATUS, Constants.SUCCESS);


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

            result.put(Constants.STATUS, Constants.FAIL);

        } finally {

            if (connection != null) {

                connection.close();

            }

        }

        return result;

    }

    void createTable() {

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            Statement stmt = con.createStatement();

            DatabaseMetaData dbm = con.getMetaData();

            ResultSet tables = dbm.getTables(null, null, "Credentials", null);

            ResultSet resultSet = dbm.getTables(null, null, "Discovery", null);

            if (!tables.next()) {

                stmt.executeUpdate("create table Credentials (credentialId varchar(255),credentialName varchar(255) PRIMARY KEY,protocol varchar(255),name varchar(255),password varchar(255),community varchar(255),version varchar(255))");

            }

            if (!resultSet.next()) {

                stmt.executeUpdate("create table Discovery (discoveryId varchar(255),credentialId varchar(255),discoveryName varchar(255),ip varchar(255),type varchar(255),port int)");

            }

        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }
    }

    JsonArray getAll(String tableName,String column,String id) throws SQLException {

        JsonArray jsonArray = null;

        Connection connection = null;

        try {

            jsonArray = new JsonArray();

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query;

            if(id.equalsIgnoreCase("getall")){

                query = "select * from " + tableName;

            }else{

                query = "select * from "+   tableName  + " where "  +    column  +   " = '" + id + "'";

            }

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()) {

                JsonObject result = new JsonObject();

                if(tableName.equalsIgnoreCase(Constants.CREDENTIAL_TABLE)){

                    result.put(Constants.CREDENTIAL_ID, resultSet.getString(1));

                    result.put(Constants.CREDENTIAL_NAME, resultSet.getString(2));

                    result.put(Constants.PROTOCOL, resultSet.getString(3));

                    if (resultSet.getString(3).equalsIgnoreCase("ssh") || resultSet.getString(3).equalsIgnoreCase("winrm")) {

                        result.put(Constants.NAME, resultSet.getString(4));

                        result.put(Constants.PASSWORD, resultSet.getString(5));

                    } else if (resultSet.getString(3).equalsIgnoreCase("snmp")) {

                        result.put(Constants.COMMUNITY, resultSet.getString(6));

                        result.put(Constants.VERSION, resultSet.getString(7));

                    }
                } else{

                    result.put(Constants.DISCOVERY_TABLE_ID, resultSet.getString(1));

                    result.put(Constants.CREDENTIAL_ID, resultSet.getString(2));

                    result.put(Constants.DISCOVERY_NAME, resultSet.getString(3));

                    result.put(Constants.IP_ADDRESS, resultSet.getString(4));

                    result.put(Constants.TYPE, resultSet.getString(5));

                    result.put(Constants.PROTOCOL, resultSet.getInt(6));

                }



                jsonArray.add(result);

            }


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

        } finally {

            if (connection != null) {

                connection.close();

            }
        }

        return jsonArray;

    }


    boolean delete(String tableName, String column,String id) throws SQLException {

        Connection connection = null;

        boolean result = false;

        if(id==null || tableName==null || column==null){

            return false;

        }


        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "delete from " + tableName + " where " + column  + " ='" + id + "'";

            PreparedStatement preparedStatement = connection.prepareStatement(query);

            int a = preparedStatement.executeUpdate();

            if (a > 0) {

                result = true;

            }


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

        } finally {

            if (connection != null) {

                connection.close();

            }

        }

        return result;
    }


    boolean updateCredentials(String tableName,String columnName,String id,JsonObject userData) throws SQLException {

        Connection connection = null;

        boolean result = true;

        if (!userData.containsKey(Constants.CREDENTIAL_ID)) {

            return false;

        }

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String protocolQuery = "select protocol from Credentials where credentialId = '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(protocolQuery);

            String protocol = null;

            if (resultSet.next()) {

                protocol = resultSet.getString(1);

            }

            if (protocol == null) {

                return false;

            }

            if(userData.containsKey(Constants.CREDENTIAL_NAME)){

                if(checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_TABLE_NAME,userData.getString(Constants.CREDENTIAL_NAME))){

                    return false;

                }

            }

            if(protocol.equalsIgnoreCase("ssh") || protocol.equalsIgnoreCase("winrm")){

                if(!(userData.containsKey(Constants.CREDENTIAL_NAME) || userData.containsKey(Constants.NAME) || userData.containsKey(Constants.PASSWORD))){

                    return false;

                }

            }else if(protocol.equalsIgnoreCase("snmp")){

                if(!(userData.containsKey(Constants.CREDENTIAL_NAME) || userData.containsKey(Constants.COMMUNITY) || userData.containsKey(Constants.VERSION))){

                    return false;

                }

            }


            HashMap<String,Object> data = new HashMap<>(userData.getMap());

            if(userData.containsKey(Constants.CREDENTIAL_NAME))
            {
                data.put("credentialName",data.get("credential.name"));

                data.remove("credential.name");
            }

            data.remove("credentialId");

            data.remove("protocol");

            String query;

            StringBuilder update = new StringBuilder("");

            query = "UPDATE " + tableName + " SET ";

            int count=0;

            for(Map.Entry<String,Object> map:data.entrySet()){

                if(count!=data.size()-1){

                    update.append(map.getKey()).append(" = ").append("'").append(map.getValue()).append("'").append(", ");

                }else{

                    update.append(map.getKey()).append(" = ").append("'").append(map.getValue()).append("'").append(" ");

                }

                count++;

            }

            String updatedQuery=  query + update + " where " + columnName + " = '" + id + "' ;";

            PreparedStatement preparedStatement = connection.prepareStatement(updatedQuery);

             preparedStatement.execute();


        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return false;

        } finally {

            if (connection != null) {

                connection.close();

            }
        }

        return result;

    }

    boolean updateDiscovery(String tableName,String columnName,String id, JsonObject userData) throws SQLException {

        Connection connection = null;

        boolean result = true;

        if (!userData.containsKey(Constants.DISCOVERY_TABLE_ID)) {

            return false;

        }

        if(userData.containsKey(Constants.CREDENTIAL_ID)){

            if(!checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,userData.getString(Constants.CREDENTIAL_ID))){

                return false;

            }

        }

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");


                if(!(userData.containsKey(Constants.IP_ADDRESS) || userData.containsKey(Constants.DISCOVERY_NAME) || userData.containsKey(Constants.CREDENTIAL_ID)) ){

                    return false;

                }

                if(userData.containsKey(Constants.DISCOVERY_NAME)){

                    if(checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_NAME,userData.getString(Constants.DISCOVERY_NAME))){

                        return false;

                    }

                }


            HashMap<String,Object> data = new HashMap<>(userData.getMap());


            if(userData.containsKey(Constants.DISCOVERY_NAME)){

                data.put("discoveryName",data.get("discovery.name"));

                data.remove("discovery.name");

            }

            if(userData.containsKey(Constants.IP_ADDRESS)){

                data.put("ip",data.get("ip.address"));

                data.remove("ip.address");
            }


            data.remove("discoveryId");

            data.remove("type");

            data.remove("port");

            String query;

            StringBuilder update = new StringBuilder("");

            query = "UPDATE " + tableName + " SET ";

            int count=0;

            for(Map.Entry<String,Object> map:data.entrySet()){

                if(count!=data.size()-1){

                    update.append(map.getKey()).append(" = '").append(map.getValue()).append("', ");

                }else{

                    update.append(map.getKey()).append(" = '").append(map.getValue()).append("', ");

                }

                count++;

            }

            String updatedQuery=  query + update + " where " + columnName + " = '" + id + "' ;";

            PreparedStatement preparedStatement = connection.prepareStatement(updatedQuery);

            preparedStatement.execute();


        }catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            return false;

        } finally {

            if (connection != null) {

                connection.close();

            }
        }

        return result;
    }

    JsonObject insertDiscovery(JsonObject userData) throws SQLException {

        Connection connection = null;

        JsonObject result = new JsonObject();

        if (!containsAllDiscovery(userData)) {

            return null;

        }

        try {

            if (checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_NAME,userData.getString(Constants.DISCOVERY_NAME))) {

                result.put(Constants.STATUS, Constants.FAIL);

                return result;

            }

            if(!checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,userData.getString(Constants.CREDENTIAL_ID))){

                result.put(Constants.STATUS, Constants.FAIL);

                return result;

            }


            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            PreparedStatement preparedStatement = connection.prepareStatement("insert into Discovery (discoveryId,credentialId,discoveryName,ip,type,port) values (?,?,?,?,?,?)");

            preparedStatement.setString(1, userData.getString(Constants.DISCOVERY_TABLE_ID));

            preparedStatement.setString(2, userData.getString(Constants.CREDENTIAL_ID));

            preparedStatement.setString(3, userData.getString(Constants.DISCOVERY_NAME));

            preparedStatement.setString(4, userData.getString(Constants.IP_ADDRESS));

            preparedStatement.setString(5, userData.getString(Constants.TYPE));

            preparedStatement.setInt(6, userData.getInteger(Constants.PORT));

            preparedStatement.executeUpdate();

            result.put(Constants.STATUS, Constants.SUCCESS);


        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

            result.put(Constants.STATUS, Constants.FAIL);

        } finally {

            if (connection != null) {

                connection.close();

            }

        }

        return result;

    }


    static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        createTable();

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_CHECK_NAME, dataHandler -> {

            vertx.executeBlocking(handler -> {

                try {

                    JsonObject data = dataHandler.body();

                    if (!checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_TABLE_NAME,data.getString(Constants.CREDENTIAL_NAME))) {

                        handler.complete(data);

                    } else {

                        handler.fail(Constants.FAIL);

                    }

                } catch (Exception exception) {

                    handler.fail(exception.getMessage());

                }


            }).onComplete(resultHandler -> {

                if (resultHandler.succeeded()) {

                    dataHandler.reply(resultHandler.result());


                } else {

                    dataHandler.fail(-1, Constants.FAIL);

                }

            });

        });


        vertx.eventBus().<JsonObject>consumer(Constants.CREDENTIAL_PUT_NAME_CHECK,handler -> {

            vertx.executeBlocking(blockingHandler->{

                JsonObject userData = handler.body();

                try {

                    if(checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,userData.getString(Constants.CREDENTIAL_ID))){

                        blockingHandler.complete();

                    }else{

                        blockingHandler.fail(Constants.FAIL);

                    }

                } catch (Exception exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    blockingHandler.fail(Constants.FAIL);

                }


            }).onComplete(resultHandler->{

                if(resultHandler.succeeded()){

                    handler.reply(Constants.SUCCESS);

                }else{

                    handler.fail(-1,Constants.FAIL);

                }


            });

        });

        vertx.eventBus().<String>consumer(Constants.CREDENTIAL_GET_NAME_CHECK,handler -> {

                    vertx.executeBlocking(blockingHandler -> {

                        String id = handler.body();

                        try {

                            if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, id)) {

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

                            handler.fail(-1, Constants.FAIL);

                        }


                    });
                });

        vertx.eventBus().<String>consumer(Constants.CREDENTIAL_DELETE_NAME_CHECK,handler -> {

            vertx.executeBlocking(blockingHandler -> {

                String id = handler.body();

                try {

                    if(!checkName(Constants.DISCOVERY_TABLE,Constants.CREDENTIAL_ID,id)) {

                        if (checkName(Constants.CREDENTIAL_TABLE, Constants.CREDENTIAL_ID, id)) {

                            blockingHandler.complete();

                        } else {

                            blockingHandler.fail(Constants.NOT_PRESENT);

                        }
                    }else{

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

        });


        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_CREDENTIAL_INSERT, handler -> {

            JsonObject userData = handler.body();

            userData.put("credentialId", UUID.randomUUID().toString());

            vertx.executeBlocking(request -> {

                JsonObject result;

                try {

                    result = insertCredentials(userData);

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

                    handler.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<JsonArray>consumer(Constants.DATABASE_CREDENTIAL_GET_ALL, consumer -> {

            vertx.executeBlocking(handler -> {

                try {

                    String get = "getAll";

                    JsonArray jsonArray = getAll(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,get);

                    handler.complete(jsonArray);

                } catch (SQLException exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    handler.fail(Constants.FAIL);

                }

            }).onComplete(completionHandler -> {

                if (completionHandler.succeeded()) {

                    consumer.reply(completionHandler.result());

                } else {

                    consumer.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<String>consumer(Constants.DATABASE_CREDENTIAL_DELETE, handler -> {

            vertx.executeBlocking(request -> {

                boolean result;

                try {

                    result = delete(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,handler.body());

                    if (result) {

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

                    handler.reply(Constants.SUCCESS);
                } else {

                    handler.fail(-1, Constants.FAIL);

                }
            });

        });


        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_CREDENTIAL_UPDATE, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                JsonObject userData = handler.body();

                boolean result;

                try {

                    result = updateCredentials(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,userData.getString(Constants.CREDENTIAL_ID),userData);

                    if (result) {

                        blockingHandler.complete();
                    }
                    else {

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

                    handler.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<String>consumer(Constants.DATABASE_CREDENTIAL_GET_ID, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                String id = handler.body();

                try {

                    JsonArray result = getAll(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,id);

                    if (result != null) {

                        blockingHandler.complete(result);

                    } else {

                        blockingHandler.fail(Constants.FAIL);

                    }

                } catch (Exception exception) {

                    blockingHandler.fail(exception.getMessage());

                }


            }).onComplete(resultHandler -> {

                if (resultHandler.succeeded()) {

                    handler.reply(resultHandler.result());


                } else {

                    handler.fail(-1, resultHandler.cause().toString());

                }

            });

        });


//-------------------------------------------------------------------------------------------------------------------------------------------------------------------

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_DISCOVERY_CHECK_NAME, dataHandler -> {

            vertx.executeBlocking(blockingHandler -> {

                try {

                    JsonObject data = dataHandler.body();

                    if (checkName(Constants.CREDENTIAL_TABLE,Constants.CREDENTIAL_ID,data.getString(Constants.CREDENTIAL_ID))) {

                        if(!checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_NAME,data.getString(Constants.DISCOVERY_NAME))) {

                            blockingHandler.complete(data);

                        }else{

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

                    dataHandler.reply(resultHandler.result());


                } else {

                    dataHandler.fail(-1, resultHandler.cause().getMessage());

                }

            });
        });

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_DISCOVERY_INSERT, handler -> {

            JsonObject userData = handler.body();

            userData.put(Constants.DISCOVERY_TABLE_ID, UUID.randomUUID().toString());

            vertx.executeBlocking(request -> {

                JsonObject result;

                try {

                    result = insertDiscovery(userData);

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

        });

        vertx.eventBus().<String>consumer(Constants.DATABASE_DISCOVERY_DELETE, handler -> {

            vertx.executeBlocking(request -> {

                boolean result;

                try {

                    String id = handler.body();

                    result = delete(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,id);

                    if (result) {

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

                    handler.reply(Constants.SUCCESS);
                } else {

                    handler.fail(-1, Constants.FAIL);

                }
            });

        });

        vertx.eventBus().<JsonArray>consumer(Constants.DATABASE_DISCOVERY_GET_ALL, consumer -> {

            vertx.executeBlocking(handler -> {

                try {

                    String id = "getAll";

                    JsonArray jsonArray = getAll(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,id);

                    handler.complete(jsonArray);

                } catch (SQLException exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    handler.fail(Constants.FAIL);

                }

            }).onComplete(completionHandler -> {

                if (completionHandler.succeeded()) {

                    consumer.reply(completionHandler.result());

                } else {

                    consumer.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<String>consumer(Constants.DATABASE_DISCOVERY_GET_ID, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                String id = handler.body();

                try {

                    JsonArray result = getAll(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,id);

                    blockingHandler.complete(result);

                } catch (Exception exception) {

                    blockingHandler.fail(exception.getMessage());

                }


            }).onComplete(resultHandler -> {

                if (resultHandler.succeeded()) {

                    handler.reply(resultHandler.result());


                } else {

                    handler.fail(-1, resultHandler.cause().toString());

                }

            });

        });

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_DISCOVERY_UPDATE, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                JsonObject userData = handler.body();

                boolean result;

                try {

                    result = updateDiscovery(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,userData.getString(Constants.DISCOVERY_TABLE_ID),userData);

                    if (result) {

                        blockingHandler.complete();
                    }
                    else {

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

                    handler.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<JsonObject>consumer(Constants.DISCOVERY_PUT_NAME_CHECK,handler -> {

            vertx.executeBlocking(blockingHandler->{

                JsonObject userData = handler.body();

                try {

                    if(checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,userData.getString(Constants.DISCOVERY_TABLE_ID))){

                        blockingHandler.complete();

                    }else{

                        blockingHandler.fail(Constants.FAIL);

                    }

                } catch (Exception exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    blockingHandler.fail(Constants.FAIL);

                }


            }).onComplete(resultHandler->{

                if(resultHandler.succeeded()){

                    handler.reply(Constants.SUCCESS);

                }else{

                    handler.fail(-1,Constants.FAIL);

                }


            });

        });

        vertx.eventBus().<String>consumer(Constants.DISCOVERY_GET_NAME_CHECK,handler -> {

            vertx.executeBlocking(blockingHandler->{

                String id = handler.body();

                try {


                    if(checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,id)){

                        blockingHandler.complete();

                    }else{

                        blockingHandler.fail(Constants.NOT_PRESENT);

                    }

                } catch (Exception exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    blockingHandler.fail(Constants.FAIL);

                }


            }).onComplete(resultHandler->{

                if(resultHandler.succeeded()){

                    handler.reply(Constants.SUCCESS);

                }else{

                    handler.fail(-1,resultHandler.cause().getMessage());

                }


            });

        });



        vertx.eventBus().<String>consumer(Constants.DISCOVERY_DELETE_NAME_CHECK,handler -> {

            vertx.executeBlocking(blockingHandler->{

                String id = handler.body();

                try {



                    if(checkName(Constants.DISCOVERY_TABLE,Constants.DISCOVERY_TABLE_ID,id)){

                        blockingHandler.complete();

                    }else{

                        blockingHandler.fail(Constants.NOT_PRESENT);

                    }

                } catch (Exception exception) {

                    LOG.debug("Error {} ", exception.getMessage());

                    blockingHandler.fail(Constants.FAIL);

                }


            }).onComplete(resultHandler->{

                if(resultHandler.succeeded()){

                    handler.reply(Constants.SUCCESS);

                }else{

                    handler.fail(-1,resultHandler.cause().getMessage());

                }


            });

        });

        startPromise.complete();

    }
}
