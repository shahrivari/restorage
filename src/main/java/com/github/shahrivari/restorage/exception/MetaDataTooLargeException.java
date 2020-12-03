package com.github.shahrivari.restorage.exception;

public class MetaDataTooLargeException extends ReStorageException {

    public static final int ERROR_CODE = 1005;

    public MetaDataTooLargeException(String bucket, String key) {
        super("Metadata is too large: " + bucket + ":" + key, 404, ERROR_CODE);
    }
}
