package com.mindarray;

import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ProcessHandler extends NuAbstractProcessHandler {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());
    StringBuilder stringBuilder = new StringBuilder();

    StringBuilder error = new StringBuilder();

    private NuProcess nuProcess;
    private String result = null;

    public void onStart(NuProcess nuProcess) {

        this.nuProcess = nuProcess;

    }

    public void onExit(int statusCode) {

    }

    public void onStdout(ByteBuffer buffer, boolean closed) {

        try {

            if (!closed) {

                byte[] bytes = new byte[buffer.remaining()];

                buffer.get(bytes);

                result = new String(bytes);

                stringBuilder.append(result);

                nuProcess.closeStdin(true);

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

        }

    }

    public void onStderr(ByteBuffer buffer, boolean closed) {

        try {

            if (!closed) {

                byte[] bytes = new byte[buffer.remaining()];

                buffer.get(bytes);

                result = new String(bytes);

                stringBuilder.append(result);

                nuProcess.closeStdin(true);

            }

        } catch (Exception exception) {

            LOG.debug("Error {}", exception.getMessage());

        }
    }

    public String output() {

        return stringBuilder.toString();

    }

}