package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.io.File
import java.io.FileOutputStream
import java.util.*

data class BucketInfo(val name: String, val timestamp: Long = System.currentTimeMillis())

class BucketMetaDataStore(val rootDir: String) {

    private val bucketCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build<String, Optional<BucketInfo>>(CacheLoader.from { bucket: String? ->
                val file = File(getBucketFilePath(bucket ?: error("Bucket is null!")))
                if (!file.exists()) return@from Optional.empty()
                return@from Optional.ofNullable(fromJson<BucketInfo>(file.readText()))
            })


    private fun getBucketFilePath(bucket: String): String {
        require(bucket.all { it.isLetterOrDigit() || "_-.=".contains(it) }) {
            "Bucket name must be alphanumerical or any of {_, -, ., =} but is: $bucket"
        }

        return DirectoryCalculator.getTwoNestedLevels(rootDir, bucket) + ".bucket"
    }

    fun createBucket(bucket: String): BucketInfo {
        if (bucketExists(bucket))
            throw BucketAlreadyExists(bucket)

        val bucketInfo = BucketInfo(bucket)
        bucketCache.put(bucket, Optional.of(bucketInfo))
        synchronized(getBucket(bucket)) {
            FileOutputStream(getBucketFilePath(bucket)).writer(Charsets.UTF_8).use {
                it.write(bucketInfo.toJson())
            }
        }

        return bucketInfo
    }

    private fun bucketExists(bucket: String): Boolean =
            getBucket(bucket).isPresent

    fun getBucket(bucket: String): Optional<BucketInfo> =
            bucketCache.get(bucket)


    fun deleteBucket(bucket: String) {
        if (!bucketExists(bucket))
            throw BucketNotFound(bucket)

        synchronized(getBucket(bucket)) {
            File(getBucketFilePath(bucket)).delete()
            bucketCache.invalidate(bucket)
        }
    }

}