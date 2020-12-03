package com.github.shahrivari.restorage.exception;

public class InvalidRangeRequestException extends ReStorageException {

    public static final int ERROR_CODE = 1004;

    public InvalidRangeRequestException(String header) {
        super("Invalid range request: " + header, 416, ERROR_CODE);
    }
}
