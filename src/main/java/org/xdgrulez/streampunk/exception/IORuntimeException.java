package org.xdgrulez.streampunk.exception;

public class IORuntimeException extends RuntimeException {
    public IORuntimeException(String messageString) {
        super(messageString);
    }

    public IORuntimeException(Throwable causeThrowable) {
        super(causeThrowable);
    }

    public IORuntimeException(String messageString, Throwable causeThrowable) {
        super(messageString, causeThrowable);
    }
}
