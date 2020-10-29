package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.hash.Hashing
import java.io.File
import java.io.FileOutputStream
import java.util.*

data class BucketInfo(val name: String, val timestamp: Long = System.currentTimeMillis())

class BucketMetaDataStore(val rootDir: String) {

    private val bucketCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build<String, Optional<BucketInfo>>(CacheLoader.from { bucket: String? ->
                return@from File(getBucketFilePath(bucket!!)).useLines { lines ->
                    val info = lines.map { fromJson<BucketInfo>(it) }.firstOrNull { it.name == bucket }
                    return@useLines Optional.ofNullable(info)
                }
            })


    private fun getBucketFilePath(bucket: String): String {
        val bucketSha1 = Hashing.sha256().hashString(bucket, Charsets.UTF_8).toString()
        return "$rootDir${File.separator}${bucketSha1.substring(0, 2)}${File.separator}buckets"
    }

    fun createBucket(bucket: String) {
        if (bucketExists(bucket))
            throw BucketAlreadyExists(bucket)

        bucketCache.invalidate(bucket)
        FileOutputStream(getBucketFilePath(bucket), true).bufferedWriter().use {
            it.appendLine(BucketInfo(bucket).toJson())
        }
    }

    fun bucketExists(bucket: String): Boolean =
            bucketCache.get(bucket).isPresent


    fun deleteBucket(bucket: String) {
        if (!bucketExists(bucket))
            throw BucketNotFound(bucket)

        val file = File(getBucketFilePath(bucket))
        val newList = file.useLines { lines ->
            return@useLines lines.map { fromJson<BucketInfo>(it) }.filter { it.name != bucket }
        }

        file.outputStream().bufferedWriter().use { writer ->
            newList.forEach { writer.appendLine(it.toJson()) }
        }
    }

}