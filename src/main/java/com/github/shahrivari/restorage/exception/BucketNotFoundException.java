package com.github.shahrivari.restorage.exception;

public class BucketNotFoundException extends ReStorageException {

    public static final int ERROR_CODE = 1002;

    public BucketNotFoundException(String bucket) {
        super("Bucket not found: " + bucket, 404, ERROR_CODE);
    }
}
