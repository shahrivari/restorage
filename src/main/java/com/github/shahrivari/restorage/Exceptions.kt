package com.github.shahrivari.restorage

open class StoraException : Exception {
    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}

open class StoraValidationException : StoraException {
    var statusCode: Int
    var errorCode: Int

    constructor(message: String, statusCode: Int, errorCode: Int) : super(message) {
        this.statusCode = statusCode
        this.errorCode = errorCode
    }
}


class BucketAlreadyExists(bucket: String) :
        StoraValidationException("Bucket already exists: $bucket", 409, 1001)

class BucketNotFound(bucket: String) :
        StoraValidationException("Bucket not found: $bucket", 404, 1002)

class KeyNotFoundException(bucket: String, key: String) :
        StoraValidationException("Key not found: $bucket : $key", 404, 1003)

class InvalidRangeRequest(header: String) :
        StoraValidationException("Invalid range request: $header", 416, 1004)
