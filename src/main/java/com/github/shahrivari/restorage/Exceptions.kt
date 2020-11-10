package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.fromJson

open class ReStorageException : Exception {
    var statusCode: Int
    var errorCode: Int

    constructor(message: String, statusCode: Int, errorCode: Int) : super(message) {
        this.statusCode = statusCode
        this.errorCode = errorCode
    }

    companion object {
        fun fromJson(json: String, bucket: String, key: String?): ReStorageException {
            val exception = fromJson<ReStorageException>(json)
            return when (exception.errorCode) {
                1001 -> BucketAlreadyExists(bucket)
                1002 -> BucketNotFound(bucket)
                1003 -> KeyNotFoundException(bucket, key ?: "")
                else -> exception
            }
        }
    }
}


class BucketAlreadyExists(bucket: String) :
        ReStorageException("Bucket already exists: $bucket", 409, 1001) {
}

class BucketNotFound(bucket: String) :
        ReStorageException("Bucket not found: $bucket", 404, 1002)

class KeyNotFoundException(bucket: String, key: String) :
        ReStorageException("Key not found: $bucket : $key", 404, 1003)

class InvalidRangeRequest(header: String) :
        ReStorageException("Invalid range request: $header", 416, 1004)
