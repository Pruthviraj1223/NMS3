package com.mindarray;

import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.mindarray.Constants.*;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());

    public static JsonObject checkAvailability(String ip) {

        JsonObject outcome = new JsonObject();

        if (ip == null) {

            return outcome.put(Constants.ERROR, "ip is null");

        }

        NuProcess nuProcess = null;

        try {

            List<String> commands = new ArrayList<>();

            commands.add("fping");

            commands.add("-q");

            commands.add("-c");

            commands.add("3");

            commands.add(ip);

            NuProcessBuilder processBuilder = new NuProcessBuilder(commands);

            ProcessHandler handler = new ProcessHandler();

            processBuilder.setProcessListener(handler);

            nuProcess = processBuilder.start();

            // timeout = packet * time
            // you can destroy as well

            nuProcess.waitFor(4000, TimeUnit.MILLISECONDS);

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

        } catch (Exception exception) {

            LOG.debug("Error {} ", exception.getMessage());

            outcome.put(Constants.ERROR, exception.getMessage());

        } finally {

            if (nuProcess != null) {

                nuProcess.destroy(true);

            }

        }

        return outcome;

    }

    public static JsonObject spawnProcess(JsonObject data) {

        JsonObject result = new JsonObject();

        if (data == null) {

            return result.put(Constants.ERROR, "Data is null");

        }

        NuProcess nuProcess = null;

        try {

            String encodedString = Base64.getEncoder().encodeToString(data.toString().getBytes());

            NuProcessBuilder processBuilder = new NuProcessBuilder("./plugin.exe", encodedString);

            ProcessHandler handler = new ProcessHandler();

            processBuilder.setProcessListener(handler);

            nuProcess = processBuilder.start();

            nuProcess.waitFor(15000, TimeUnit.MILLISECONDS);

            String outcome = handler.output();

            if (!outcome.isEmpty()) {

                outcome = outcome.replace("\\\"","");

                result = new JsonObject(outcome);

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

            result.put(Constants.ERROR, exception.getMessage());

        } finally {

            if (nuProcess != null) {

                nuProcess.destroy(true);

            }

        }

        return result;

    }

    public static HashMap<String, Integer> metric(String type) {

        HashMap<String, Integer> temp = new HashMap<>();

        if (type.equalsIgnoreCase(LINUX) || type.equalsIgnoreCase(WINDOWS)) {

            temp.put("cpu", 80000);

            temp.put("disk", 120000);

            temp.put("memory", 100000);

            temp.put("process", 80000);

            temp.put("SystemInfo", 200000);

            temp.put("ping", 60000);

        } else if (type.equalsIgnoreCase(Constants.NETWORKING)) {

            temp.put("SystemInfo", 200000);

            temp.put("interface", 80000);

            temp.put("ping", 60000);

        }

        return temp;

    }

}
