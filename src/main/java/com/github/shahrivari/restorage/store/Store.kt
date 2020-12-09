package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.store.fs.BucketInfo
import java.io.InputStream
import java.util.*

interface Store {

    fun createBucket(bucket: String): BucketInfo

    fun getBucketInfo(bucket: String): Optional<BucketInfo>

    fun deleteBucket(bucket: String)

    fun put(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult

    fun append(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult

    fun getMeta(bucket: String, key: String): MetaData

    fun get(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult

    fun delete(bucket: String, key: String)

    fun computeMd5(bucket: String, key: String): String
}