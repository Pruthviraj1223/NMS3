package com.mindarray;

import com.mindarray.api.ProcessHandler;
import com.zaxxer.nuprocess.NuProcessBuilder;

import io.vertx.core.json.JsonObject;

import java.util.ArrayList;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Utils {

    static JsonObject ping(String ip) {

        JsonObject outcome = new JsonObject();

        List<String> commands = new ArrayList<>();

        commands.add("fping");

        commands.add("-q");

        commands.add("-c");

        commands.add("3");

        commands.add(ip);

        NuProcessBuilder processBuilder = new NuProcessBuilder(commands);

        ProcessHandler handler = new ProcessHandler();

        processBuilder.setProcessListener(handler);

        com.zaxxer.nuprocess.NuProcess nuProcess = processBuilder.start();

        try {
            nuProcess.waitFor(0, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {

            throw new RuntimeException(e);

        }

        String result = handler.output();

        if (result != null) {

            String[] packetData = result.split(":")[1].split("=")[1].split(",")[0].split("/");

            String packetLoss = packetData[2].substring(0, packetData[2].length() - 1);

            if (packetLoss.equalsIgnoreCase("0")) {

                outcome.put(Constants.STATUS, Constants.SUCCESS);

            } else {

                outcome.put(Constants.STATUS, Constants.FAIL);

            }

        }


        return outcome;

    }


    static  JsonObject plugin(JsonObject data){

        String encodedString = Base64.getEncoder().encodeToString(data.toString().getBytes());

        NuProcessBuilder processBuilder = new NuProcessBuilder("/home/pruthviraj/NMS3/plugin.exe",encodedString);

        ProcessHandler handler = new ProcessHandler();

        processBuilder.setProcessListener(handler);

        com.zaxxer.nuprocess.NuProcess nuProcess = processBuilder.start();

        try {

            nuProcess.waitFor(0, TimeUnit.MILLISECONDS);

        } catch (InterruptedException exception) {

            System.out.println(exception.getMessage());

        }

        String outcome = handler.output();

        JsonObject result = null;

        if(outcome!=null){

            result = new JsonObject(outcome);

        }

        return result;

    }

}
