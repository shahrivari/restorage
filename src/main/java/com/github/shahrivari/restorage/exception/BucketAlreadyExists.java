package com.github.shahrivari.restorage.exception;

public class BucketAlreadyExists extends ReStorageException {

    public BucketAlreadyExists(String bucket) {
        super("Bucket already exists: " + bucket, 409, 1001);
    }
}
