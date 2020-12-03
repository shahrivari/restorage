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
            case BucketAlreadyExistsException.ERROR_CODE:
                exp = new BucketAlreadyExistsException(bucket);
                break;
            case BucketNotFoundException.ERROR_CODE:
                exp = new BucketNotFoundException(bucket);
                break;
            case KeyNotFoundException.ERROR_CODE:
                exp = new KeyNotFoundException(bucket, key == null ? "" : key);
                break;
            case InvalidRangeRequestException.ERROR_CODE:
                exp = new InvalidRangeRequestException("");
                break;
            case MetaDataTooLargeException.ERROR_CODE:
                exp = new MetaDataTooLargeException(bucket, key == null ? "" : key);
                break;
        }

        return exp;
    }
}