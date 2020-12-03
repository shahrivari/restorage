package com.github.shahrivari.restorage.exception;

public class KeyNotFoundException extends ReStorageException {

    public static final int ERROR_CODE = 1003;

    public KeyNotFoundException(String bucket, String key) {
        super("Key not found: " + bucket + ":" + key, 404, ERROR_CODE);
    }
}
