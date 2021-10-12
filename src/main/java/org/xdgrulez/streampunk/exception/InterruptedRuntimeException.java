package org.xdgrulez.streampunk.exception;

public class InterruptedRuntimeException extends RuntimeException {
    public InterruptedRuntimeException(String messageString) {
        super(messageString);
    }

    public InterruptedRuntimeException(Throwable causeThrowable) {
        super(causeThrowable);
    }

    public InterruptedRuntimeException(String messageString, Throwable causeThrowable) {
        super(messageString, causeThrowable);
    }
}
