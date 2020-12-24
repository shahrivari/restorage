package com.github.shahrivari.restorage.exception;

public class LimitedDeleteAccess extends ReStorageException {

    public static final int ERROR_CODE = 1006;

    public LimitedDeleteAccess(String bucket, String key) {
        super("Can not delete file with bucket:" +bucket + " and key: "+key , 403, ERROR_CODE);
    }
}
