package com.github.shahrivari.restorage.exception;

public class NoVideoFileException extends ReStorageException {

    public static final int ERROR_CODE = 1007;

    public NoVideoFileException(String bucket, String key) {
        super("There is no video stream in file: " + bucket + ":" + key, 404, ERROR_CODE);
    }
}
