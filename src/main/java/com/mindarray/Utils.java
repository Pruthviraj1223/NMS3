package com.mindarray;

import com.zaxxer.nuprocess.NuProcess;

import com.zaxxer.nuprocess.NuProcessBuilder;

import io.vertx.core.json.JsonObject;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.util.*;

import java.util.concurrent.TimeUnit;

public class Utils {

    static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());

    public static JsonObject ping(String ip) {

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

        NuProcess nuProcess = processBuilder.start();

        // timeout = packet * time
        // you can destroy as well

        try {

            nuProcess.waitFor(0, TimeUnit.MILLISECONDS);

        } catch (Exception exception) {

            LOG.debug("Error {} ",exception.getMessage());

            return new JsonObject().put(Constants.ERROR,exception.getMessage());

        }

        String result = handler.output();

        if (!result.isEmpty()) {

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

    public static JsonObject plugin(JsonObject data){

        String encodedString = Base64.getEncoder().encodeToString(data.toString().getBytes());

        NuProcessBuilder processBuilder = new NuProcessBuilder("./plugin.exe",encodedString);

        ProcessHandler handler = new ProcessHandler();

        processBuilder.setProcessListener(handler);

        NuProcess nuProcess = processBuilder.start();

        try {

            nuProcess.waitFor(60000, TimeUnit.MILLISECONDS);

        } catch (Exception exception) {

            LOG.debug("Error {}",exception.getMessage());

        }

        String outcome = handler.output();

        JsonObject result = null;

        if(outcome!=null){

            try {

                result = new JsonObject(outcome);

            }catch (Exception exception){

                LOG.debug("Error {}",exception.getMessage());

                return new JsonObject().put(Constants.ERROR,exception.getMessage());

            }

        }

        return result;

    }

    public static HashMap<String, Integer> metric(String type){

        HashMap<String, Integer> temp = new HashMap<>();

        if(type.equalsIgnoreCase("linux") || type.equalsIgnoreCase("windows")){

            temp.put("cpu",60000);

            temp.put("disk",120000);

            temp.put("memory",40000);

            temp.put("process",20000);

            temp.put("SystemInfo",200000);

            temp.put("ping",60000);

        }else if(type.equalsIgnoreCase(Constants.NETWORKING)){

            temp.put("systemInfo",300000);

            temp.put("interface",20000);

            temp.put("ping",60000);

        }

        return temp;
    }

}
