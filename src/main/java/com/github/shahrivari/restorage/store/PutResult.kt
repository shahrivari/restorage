package com.github.shahrivari.restorage.store

data class PutResult(
    val bucket: String,
    val key: String,
    val bytesReceived: Long? = null,
    val contentType: String? = null,
    val lastModified: Long? = System.currentTimeMillis()
)

