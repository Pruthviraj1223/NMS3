package com.mindarray.api;

import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;

import java.nio.ByteBuffer;

public class ProcessHandler extends NuAbstractProcessHandler {
    private NuProcess nuProcess;
    private String result;

    @Override
    public void onStart(NuProcess nuProcess) {

        this.nuProcess = nuProcess;

    }

    public void onStdout(ByteBuffer buffer, boolean closed) {
        byte[] bytes;

        if (!closed) {

            bytes = new byte[buffer.remaining()];

            buffer.get(bytes);

            result = new String(bytes);

            nuProcess.closeStdin(true);

        }

    }
    public void onStderr(ByteBuffer buffer, boolean closed){

        byte[] bytes;

        if (!closed) {

            bytes = new byte[buffer.remaining()];

            buffer.get(bytes);

            result = new String(bytes);

            nuProcess.closeStdin(true);

        }

    }
    public String output() {

        return result;

    }

}