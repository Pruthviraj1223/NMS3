package com.mindarray;

import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ProcessHandler extends NuAbstractProcessHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());

    private final StringBuilder stringBuilder = new StringBuilder();

    private NuProcess nuProcess;

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

                stringBuilder.append(new String(bytes));

                nuProcess.closeStdin(true);

            }

        } catch (Exception exception) {

            LOG.error(exception.getMessage(),exception);

        }

    }

    public void onStderr(ByteBuffer buffer, boolean closed) {

        try {

            if (!closed) {

                byte[] bytes = new byte[buffer.remaining()];

                buffer.get(bytes);

                stringBuilder.append(new String(bytes));

                nuProcess.closeStdin(true);

            }

        } catch (Exception exception) {

            LOG.error(exception.getMessage(),exception);

        }
    }

    public String output() {

        return stringBuilder.toString();

    }

}