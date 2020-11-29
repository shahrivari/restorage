package com.github.shahrivari.restorage.exception;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.kotlin.KotlinModule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReStorageException extends Exception {
    private final int statusCode;
    private final int errorCode;

    public final int getStatusCode() {
        return this.statusCode;
    }


    public final int getErrorCode() {
        return this.errorCode;
    }

    public ReStorageException() {
        statusCode = 0;
        errorCode = 0;
    }

    public ReStorageException(String message, int statusCode, int errorCode) {
        super(message);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
    }

    private static ObjectMapper mapper = new ObjectMapper().registerModule(new KotlinModule());

    public static ReStorageException fromJson(String json,
                                              String bucket,
                                              String key) throws JsonProcessingException {
        ReStorageException exp = mapper.readValue(json, ReStorageException.class);

        switch (exp.errorCode) {
            case 1001:
                exp = new BucketAlreadyExists(bucket);
                break;
            case 1002:
                exp = new BucketNotFound(bucket);
                break;
            case 1003:
                exp = new KeyNotFoundException(bucket, key == null ? "" : key);
                break;
        }

        return exp;
    }
}