package com.github.shahrivari.restorage.exception;

public class InvalidRangeRequest extends ReStorageException {

    public InvalidRangeRequest(String header) {
        super("Invalid range request: " + header, 416, 1004);
    }
}
