package com.mindarray;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class DatabaseEngine extends AbstractVerticle {

    boolean checkName(JsonObject entries) throws SQLException {

        Connection connection = null;

        boolean isAvailable = false;

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

            String query = "select * from Credentials where credentialName='" + entries.getString(Constants.CREDENTIAL_NAME) + "'";

            ResultSet resultSet = connection.createStatement().executeQuery(query);

            isAvailable = resultSet.next();

        } catch (Exception exception){

            LOG.debug("Error : {}" , exception.getMessage());

        }
        finally {

            if(connection!=null){

                connection.close();

            }

        }

        return isAvailable;

    }

    JsonObject insert(JsonObject userData) throws SQLException {

        Connection connection = null;

        JsonObject result = new JsonObject();

        try {

            if(checkName(userData)){

                result.put(Constants.STATUS,Constants.FAIL);

                return result;

            }

                Class.forName("com.mysql.cj.jdbc.Driver");

                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/NMS", "root", "password");

                PreparedStatement preparedStatement = connection.prepareStatement("insert into Credentials (credentialId,credentialName,protocol,username,password,community,version) values (?,?,?,?,?,?,?)");

                preparedStatement.setString(1, userData.getString(Constants.CREDENTIAL_ID));

                preparedStatement.setString(2, userData.getString(Constants.CREDENTIAL_NAME));

                preparedStatement.setString(3, userData.getString(Constants.PROTOCOL));

                preparedStatement.setString(4, userData.getString(Constants.NAME));

                preparedStatement.setString(5, userData.getString(Constants.PASSWORD));

                preparedStatement.setString(6, userData.getString(Constants.COMMUNITY));

                preparedStatement.setString(7, userData.getString(Constants.VERSION));

                preparedStatement.executeUpdate();

                result.put(Constants.STATUS,Constants.SUCCESS);


        }catch (Exception exception){

            LOG.debug("Error : {} ", exception.getMessage());

            result.put(Constants.STATUS,Constants.FAIL);

        }

        finally {

            if(connection!=null){

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

                stmt.executeUpdate("create table Credentials (credentialId varchar(255),credentialName varchar(255) PRIMARY KEY,protocol varchar(255),username varchar(255),password varchar(255),community varchar(255),version varchar(255))");

            }

            if (!resultSet.next()) {

                stmt.executeUpdate("create table Discovery (discoveryName varchar(255),ip varchar(255),type varchar(255),credentialId varchar(255),port int)");

            }

        }catch (Exception exception){

            LOG.debug("Error : {} " , exception.getMessage());

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

            while (resultSet.next()){

                JsonObject jsonObject = new JsonObject();
              
                jsonObject.put(Constants.CREDENTIAL_ID,resultSet.getString(1));
              
                jsonObject.put(Constants.CREDENTIAL_NAME,resultSet.getString(2));
              
                jsonObject.put(Constants.PROTOCOL,resultSet.getString(3));
               
                jsonObject.put(Constants.NAME,resultSet.getString(4));
               
                jsonObject.put(Constants.PASSWORD,resultSet.getString(5));
                
                jsonArray.add(jsonObject);

            }


        }catch (Exception exception){

            LOG.debug("Error {} ", exception.getMessage());
            
        }

        finally {
            if(connection!=null){
                connection.close();
            }
        }

        return jsonArray;

    }



    static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise)  {

        createTable();

        vertx.eventBus().<JsonObject>consumer(Constants.DATABASE_INSERT,handler->{

            JsonObject userData = handler.body();

            userData.put("credentialId", UUID.randomUUID().toString());

            vertx.executeBlocking(request -> {

                JsonObject result;

                try {

                    result = insert(userData);

                    if(result.getString(Constants.STATUS).equalsIgnoreCase(Constants.SUCCESS)){

                        request.complete();

                    }else{

                        request.fail(Constants.FAIL);

                    }


                } catch (Exception exception) {

                    LOG.debug("Error : {}" + exception.getMessage());

                    request.fail(exception.getMessage());

                }

            }).onComplete(completeHandler->{

                if(completeHandler.succeeded()){

                    handler.reply(userData);
                }else{

                    handler.fail(-1,Constants.FAIL);

                }
            });

        });

        vertx.eventBus().<JsonArray>consumer(Constants.DATABSE_GET_ALL,consumer->{

           vertx.executeBlocking(handler->{

               try {
                   JsonArray jsonArray = getAll();

                   handler.complete(jsonArray);

               } catch (SQLException exception) {

                   LOG.debug("Error {} ", exception.getMessage());

                   handler.fail(Constants.FAIL);

               }

           }).onComplete(completionHandler->{

               if(completionHandler.succeeded()){

                   consumer.reply(completionHandler.result());

               }else{

                   consumer.fail(-1,Constants.FAIL);

               }

           });

        });


        startPromise.complete();

    }
}
