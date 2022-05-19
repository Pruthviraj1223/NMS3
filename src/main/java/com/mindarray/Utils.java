package com.mindarray;

import com.mindarray.api.ProcessHandler;
import com.zaxxer.nuprocess.NuProcessBuilder;

import io.vertx.core.json.JsonObject;

import java.util.*;

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

        System.out.println(outcome);

        JsonObject result = null;

        if(outcome!=null){

            result = new JsonObject(outcome);

        }

        return result;

    }

    public static HashMap<String, HashMap<String, Integer>> metric(){

        HashMap<String,HashMap<String,Integer>> metricMap = new HashMap<>();

        HashMap<String, Integer> temp = new HashMap<>();

        HashMap<String,Integer> snmp = new HashMap<>();

        temp.put("cpu",60000);

        temp.put("disk",120000);

        temp.put("memory",40000);

        temp.put("process",20000);

        temp.put("SystemInfo",200000);

        metricMap.put("linux",temp);

        metricMap.put("windows",temp);

        snmp.put("systemInfo",300000);

        snmp.put("interface",20000);

        metricMap.put("networking",snmp);

        return metricMap;
    }

}
