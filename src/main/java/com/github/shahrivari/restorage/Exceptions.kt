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

    constructor(message: String,
                statusCode: Int = 400,
                errorCode: Int = 400,
                cause: Throwable) : super(message, cause) {
        this.statusCode = statusCode
        this.errorCode = errorCode
    }
}


class FileNotExistException(message: String, statusCode: Int = 404, errorCode: Int = 400) :
        StoraValidationException(message, statusCode, errorCode)

class BucketAlreadyExists(bucket: String, statusCode: Int = 409, errorCode: Int = 400) :
        StoraValidationException("Bucket already exists: $bucket", statusCode, errorCode)

class BucketNotFound(bucket: String, statusCode: Int = 404, errorCode: Int = 400) :
        StoraValidationException("Bucket not found: $bucket", statusCode, errorCode)

class KeyNotFoundException(bucket: String, key: String, statusCode: Int = 404, errorCode: Int = 400) :
        StoraValidationException("Key not found: $bucket : $key", statusCode, errorCode)

class InvalidRangeRequest(header: String) :
        StoraValidationException("Invalid range request: $header", 400, 400)
