package com.github.shahrivari.restorage.exception;

public class BucketAlreadyExistsException extends ReStorageException {

    public static final int ERROR_CODE = 1001;

    public BucketAlreadyExistsException(String bucket) {
        super("Bucket already exists: " + bucket, 409, ERROR_CODE);
    }
}
