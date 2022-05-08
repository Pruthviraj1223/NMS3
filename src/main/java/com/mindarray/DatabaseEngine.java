package com.mindarray;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import java.util.Objects;
import java.util.UUID;

public class DatabaseEngine extends AbstractVerticle {

    boolean checkName(JsonObject entries) throws SQLException {

        Connection connection = null;

        boolean isAvailable = false;

        if (!entries.containsKey(Constants.CREDENTIAL_NAME)) {
            return false;
        }

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "select * from Credentials where credentialName='" + entries.getString(Constants.CREDENTIAL_NAME) + "'";

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

    boolean containsAll(JsonObject data) {

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


    JsonObject insert(JsonObject userData) throws SQLException {

        Connection connection = null;

        JsonObject result = new JsonObject();

        if (!containsAll(userData)) {
            return null;
        }

        try {

            if (checkName(userData)) {

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

                stmt.executeUpdate("create table Discovery (discoveryName varchar(255),ip varchar(255),type varchar(255),credentialId varchar(255),port int)");

            }

        } catch (Exception exception) {

            LOG.debug("Error : {} ", exception.getMessage());

        }
    }

    JsonArray getAll() throws SQLException {

        JsonArray jsonArray = null;

        Connection connection = null;

        try {

            jsonArray = new JsonArray();

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");


            String query = "select * from Credentials";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            while (resultSet.next()) {

                JsonObject result = new JsonObject();

                result.put(Constants.CREDENTIAL_ID, resultSet.getString(1));

                result.put(Constants.CREDENTIAL_NAME, resultSet.getString(2));

                result.put(Constants.PROTOCOL, resultSet.getString(3));

                if (resultSet.getString(3).equalsIgnoreCase("ssh") || resultSet.getString(3).equalsIgnoreCase("winrm")) {
                    result.put(Constants.NAME, resultSet.getString(4));

                    result.put(Constants.PASSWORD, resultSet.getString(5));
                } else if (resultSet.getString(3).equalsIgnoreCase("snmp")) {

                    result.put(Constants.NAME, resultSet.getString(6));

                    result.put(Constants.PASSWORD, resultSet.getString(7));

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

    JsonObject getId(String id) throws SQLException {


        Connection connection = null;

        JsonObject result = null;

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "select * from Credentials where credentialId = '" + id + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            if (resultSet.next()) {

                result = new JsonObject();

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

    boolean delete(String id) throws SQLException {

        Connection connection = null;

        boolean result = false;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "delete from Credentials where credentialId='" + id + "'";

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


    boolean update(JsonObject userData) throws SQLException {

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

            String query;

            if (userData.containsKey(Constants.NAME) && userData.containsKey(Constants.PASSWORD) && protocol.equalsIgnoreCase("linux") || protocol.equalsIgnoreCase("windows")) {

                query = "update Credentials SET name = '" + userData.getString(Constants.NAME) + "' , password = '" + userData.getString(Constants.PASSWORD) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else if (userData.containsKey(Constants.NAME) && protocol.equalsIgnoreCase("linux") || protocol.equalsIgnoreCase("windows")) {

                query = "update Credentials SET name = '" + userData.getString(Constants.NAME) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else if (userData.containsKey(Constants.PASSWORD) && protocol.equalsIgnoreCase("linux") || protocol.equalsIgnoreCase("windows")) {

                query = "update Credentials SET password = '" + userData.getString(Constants.PASSWORD) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else if (userData.containsKey(Constants.COMMUNITY) && userData.containsKey(Constants.VERSION) && protocol.equalsIgnoreCase("snmp")) {

                query = "update Credentials SET community = '" + userData.getString(Constants.COMMUNITY) + "' , version = '" + userData.getString(Constants.VERSION) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else if (userData.containsKey(Constants.COMMUNITY) && protocol.equalsIgnoreCase("snmp")) {

                query = "update Credentials SET community = '" + userData.getString(Constants.COMMUNITY) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else if (userData.containsKey(Constants.VERSION) && protocol.equalsIgnoreCase("snmp")) {

                query = "update Credentials SET version = '" + userData.getString(Constants.VERSION) + "' where credentialId= '" + userData.getString(Constants.CREDENTIAL_ID) + "'";

            } else {
                return false;
            }

            PreparedStatement preparedStatement = connection.prepareStatement(query);

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


    static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        createTable();

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_CHECK_NAME, dataHandler -> {

            vertx.executeBlocking(handler -> {

                try {

                    JsonObject data = dataHandler.body();

                    if (!checkName(data)) {

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

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_INSERT, handler -> {

            JsonObject userData = handler.body();

            userData.put("credentialId", UUID.randomUUID().toString());

            vertx.executeBlocking(request -> {

                JsonObject result;

                try {

                    result = insert(userData);

                    if (result == null) {

                        request.fail(Constants.FAIL);
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

        vertx.eventBus().<JsonArray>consumer(Constants.DATABASE_GET_ALL, consumer -> {

            vertx.executeBlocking(handler -> {

                try {
                    JsonArray jsonArray = getAll();

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

        vertx.eventBus().<String>consumer(Constants.DATABASE_DELETE, handler -> {

            vertx.executeBlocking(request -> {

                boolean result;

                try {

                    result = delete(handler.body());

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


        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_UPDATE, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                JsonObject userData = handler.body();

                boolean result;

                try {

                    result = update(userData);

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

                    handler.fail(-1, Constants.FAIL);

                }

            });

        });

        vertx.eventBus().<String>consumer(Constants.DATABASE_GET_ID, handler -> {

            vertx.executeBlocking(blockingHandler -> {

                String id = handler.body();

                try {

                    JsonObject result = getId(id);

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


        startPromise.complete();

    }
}
