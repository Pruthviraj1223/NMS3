package com.mindarray;

import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mindarray.Constants.*;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());

    public static JsonObject checkAvailability(String ip) {

        JsonObject outcome = new JsonObject();

        if (ip == null) {

            return outcome.put(ERROR, NULL_DATA);

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

            LOG.error(exception.getMessage(),exception);

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

            return result.put(ERROR, NULL_DATA);

        }

        NuProcess nuProcess = null;

        try {

            String encodedString = Base64.getEncoder().encodeToString(data.toString().getBytes());

            NuProcessBuilder processBuilder = new NuProcessBuilder(PLUGIN_PATH, encodedString);

            ProcessHandler handler = new ProcessHandler();

            processBuilder.setProcessListener(handler);

            nuProcess = processBuilder.start();

            nuProcess.waitFor(15000, TimeUnit.MILLISECONDS);

            String outcome = handler.output();

            if (!outcome.isEmpty()) {

                result = new JsonObject(outcome);

            }else {

                result.put(ERROR,NULL_DATA);

            }

        } catch (Exception exception) {

            LOG.error(exception.getMessage(),exception);

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

            temp.put(CPU, 80000);

            temp.put(DISK, 120000);

            temp.put(MEMORY, 100000);

            temp.put(PROCESS, 80000);

            temp.put(SYSTEM_INFO, 200000);

            temp.put(PING, 60000);

        } else if (type.equalsIgnoreCase(Constants.NETWORKING)) {

            temp.put(SYSTEM_INFO, 200000);

            temp.put(INTERFACE, 80000);

            temp.put(PING, 60000);

        }

        return temp;

    }

}
