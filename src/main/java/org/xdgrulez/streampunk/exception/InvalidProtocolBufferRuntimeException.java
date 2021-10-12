package org.xdgrulez.streampunk.exception;

public class InvalidProtocolBufferRuntimeException extends RuntimeException {
    public InvalidProtocolBufferRuntimeException(String messageString) {
        super(messageString);
    }

    public InvalidProtocolBufferRuntimeException(Throwable causeThrowable) {
        super(causeThrowable);
    }

    public InvalidProtocolBufferRuntimeException(String messageString, Throwable causeThrowable) {
        super(messageString, causeThrowable);
    }
}
