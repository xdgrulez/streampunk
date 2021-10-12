package org.xdgrulez.streampunk.exception;

public class ExecutionRuntimeException extends RuntimeException {
    public ExecutionRuntimeException(String messageString) {
        super(messageString);
    }

    public ExecutionRuntimeException(Throwable causeThrowable) {
        super(causeThrowable);
    }

    public ExecutionRuntimeException(String messageString, Throwable causeThrowable) {
        super(messageString, causeThrowable);
    }
}
