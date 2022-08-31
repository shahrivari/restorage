package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.store.fs.BucketInfo
import java.io.InputStream
import java.util.*

interface Store {

    companion object {
        const val M3U8_FILE_NAME = "video.m3u8"
        const val TS_FILE_PATTERN = "part-%d.ts"
    }

    fun createBucket(bucket: String): BucketInfo

    fun getBucketInfo(bucket: String): Optional<BucketInfo>

    fun deleteBucket(bucket: String)

    fun put(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult

    fun append(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult

    fun getMeta(bucket: String, key: String): MetaData

    fun objectExists(bucket: String, key: String): Boolean

    fun get(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult

    fun delete(bucket: String, key: String) : Long

    fun computeMd5(bucket: String, key: String): String

    fun generateHls(bucket: String, key: String): HlsCreationResult

    fun getHlsFile(bucket: String, key: String, file: String): InputStream

}