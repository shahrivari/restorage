package com.github.shahrivari.restorage.store.fs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.github.shahrivari.restorage.exception.BucketAlreadyExistsException
import com.github.shahrivari.restorage.exception.BucketNotFoundException
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.cache.CacheBuilder
import java.io.File
import java.io.FileOutputStream
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write

data class BucketInfo(val id: Long,
                      val name: String,
                      val timestamp: Long = System.currentTimeMillis(),
                      @JsonIgnore
                      val lock: ReentrantReadWriteLock = ReentrantReadWriteLock())

class BucketMetaDataStore(val rootDir: String) {

    private val bucketCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build<String, Optional<BucketInfo>>()

    private val lastBucketIdFile = File("$rootDir${File.separator}lastBucket.id")
    private val bucketLogFile = File("$rootDir${File.separator}buckets.log")
    private val lastBucketId = AtomicLong(0)

    init {
        if (lastBucketIdFile.exists())
            lastBucketId.set(readLastBucketId())
        else
            lastBucketIdFile.writeText(lastBucketId.toString())
    }

    private fun readLastBucketId(): Long {
        return lastBucketIdFile.readText().toLongOrNull()
                ?: error("Cannot read last bucket id from file: ${lastBucketIdFile.path}")
    }

    @Synchronized
    private fun incrementAndGetLastBucketId(): Long {
        val id = lastBucketId.incrementAndGet()
        lastBucketIdFile.writeText(id.toString())
        return id
    }

    private fun logBucketOperation(operation: String, bucketInfo: BucketInfo) {
        synchronized(bucketLogFile) {
            FileOutputStream(bucketLogFile, true).writer().use {
                it.write("$operation\t${bucketInfo.id}\t${bucketInfo.name}\n")
            }
        }
    }

    private fun getBucketFilePath(bucket: String): String {
        require(bucket.all { it.isLetterOrDigit() || "_-.=".contains(it) }) {
            "Bucket name must be alphanumerical or any of {_, -, ., =} but is: $bucket"
        }

        return DirectoryCalculator.getTwoNestedLevels(rootDir, bucket) + ".bucket"
    }

    fun createBucket(bucket: String): BucketInfo {
        if (getBucket(bucket).isPresent)
            throw BucketAlreadyExistsException(bucket)

        val bucketInfo = BucketInfo(incrementAndGetLastBucketId(), bucket)
        bucketCache.put(bucket, Optional.of(bucketInfo))
        lockBucketForWrite(bucket) {
            FileOutputStream(getBucketFilePath(bucket)).writer(Charsets.UTF_8).use {
                it.write(bucketInfo.toJson())
            }
        }

        logBucketOperation("CREATE", bucketInfo)
        return bucketInfo
    }

    fun getBucket(bucket: String): Optional<BucketInfo> =
            bucketCache.get(bucket) { // Load if empty
                val file = File(getBucketFilePath(bucket))
                if (!file.exists()) return@get Optional.empty()
                return@get Optional.ofNullable(fromJson<BucketInfo>(file.readText()))

            }

    fun deleteBucket(bucket: String) {
        val bucketInfo = getBucket(bucket)
        if (!bucketInfo.isPresent)
            throw BucketNotFoundException(bucket)

        lockBucketForWrite(bucket) {
            File(getBucketFilePath(bucket)).delete()
            bucketCache.invalidate(bucket)
        }

        logBucketOperation("CREATE", bucketInfo.get())
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