package com.github.shahrivari.restorage.exception;

public class KeyNotFoundException extends ReStorageException {

    public KeyNotFoundException(String bucket, String key) {
        super("Key not found: " + bucket + ":" + key, 404, 1003);
    }
}
