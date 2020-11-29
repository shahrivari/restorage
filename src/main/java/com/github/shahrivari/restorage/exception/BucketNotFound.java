package com.github.shahrivari.restorage.exception;

public class BucketNotFound extends ReStorageException {

    public BucketNotFound(String bucket) {
        super("Bucket not found: " + bucket, 404, 1002);
    }
}
