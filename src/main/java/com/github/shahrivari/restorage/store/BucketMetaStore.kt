package com.github.shahrivari.restorage.store

import com.fasterxml.jackson.annotation.JsonIgnore
import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.io.File
import java.io.FileOutputStream
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

data class BucketInfo(val name: String,
                      val timestamp: Long = System.currentTimeMillis(),
                      @JsonIgnore
                      val lock: ReentrantReadWriteLock = ReentrantReadWriteLock())

class BucketMetaDataStore(val rootDir: String) {

    private val bucketCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build<String, Optional<BucketInfo>>()


    private fun getBucketFilePath(bucket: String): String {
        require(bucket.all { it.isLetterOrDigit() || "_-.=".contains(it) }) {
            "Bucket name must be alphanumerical or any of {_, -, ., =} but is: $bucket"
        }

        return DirectoryCalculator.getTwoNestedLevels(rootDir, bucket) + ".bucket"
    }

    fun createBucket(bucket: String): BucketInfo {
        if (getBucket(bucket).isPresent)
            throw BucketAlreadyExists(bucket)

        val bucketInfo = BucketInfo(bucket)
        bucketCache.put(bucket, Optional.of(bucketInfo))
        lockBucketForWrite(bucket) {
            FileOutputStream(getBucketFilePath(bucket)).writer(Charsets.UTF_8).use {
                it.write(bucketInfo.toJson())
            }
        }

        return bucketInfo
    }

    fun getBucket(bucket: String): Optional<BucketInfo> =
            bucketCache.get(bucket) { // Load if empty
                val file = File(getBucketFilePath(bucket ?: error("Bucket is null!")))
                if (!file.exists()) return@get Optional.empty()
                return@get Optional.ofNullable(fromJson<BucketInfo>(file.readText()))

            }

    fun deleteBucket(bucket: String) {
        if (!getBucket(bucket).isPresent)
            throw BucketNotFound(bucket)


        lockBucketForWrite(bucket) {
            File(getBucketFilePath(bucket)).delete()
            bucketCache.invalidate(bucket)
        }
    }

    private fun <R> lockBucketForWrite(bucket: String, block: () -> R): R {
        val bucketInfo = getBucket(bucket)
        if (bucketInfo.isPresent) {
            return bucketInfo.get().lock.write {
                return@write block.invoke()
            }
        } else {
            return block.invoke()
        }
    }

}