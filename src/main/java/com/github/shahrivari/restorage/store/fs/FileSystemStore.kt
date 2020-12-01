package com.github.shahrivari.restorage.store.fs

import com.github.shahrivari.restorage.commons.*
import com.github.shahrivari.restorage.exception.BucketNotFound
import com.github.shahrivari.restorage.exception.InvalidRangeRequest
import com.github.shahrivari.restorage.exception.KeyNotFoundException
import com.github.shahrivari.restorage.store.GetResult
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.PutResult
import com.github.shahrivari.restorage.store.Store
import com.google.common.cache.CacheBuilder
import java.io.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


class FileSystemStore(val rootDir: String) : Store {
    private val bucketMetaDataStore = BucketMetaDataStore(rootDir)

    private val lockCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.SECONDS)
            .build<String, ReentrantReadWriteLock>()

    init {
        val sep = File.separator
        File(rootDir).mkdirs()
        for (i in 0..255) {
            val dir1 = File("$rootDir$sep${"%02x".format(i)}")
            if (!dir1.exists()) dir1.mkdir()
            File("$rootDir$sep${"%02x".format(i)}${sep}buckets").createNewFile()
            for (j in 0..255) {
                val dir2 = File("$rootDir$sep${"%02x".format(i)}$sep${"%02x".format(j)}")
                if (!dir2.exists()) dir2.mkdir()
            }
        }
    }

    override fun createBucket(bucket: String) =
            bucketMetaDataStore.createBucket(bucket)

    override fun getBucketInfo(bucket: String): Optional<BucketInfo> =
            bucketMetaDataStore.getBucket(bucket)

    override fun deleteBucket(bucket: String) =
            bucketMetaDataStore.deleteBucket(bucket)

    override fun put(bucket: String, key: String, data: InputStream, meta: MetaData) = withBucket(
            bucket) {
        return@withBucket lockObjectForWrite(bucket, key) {
            val size = FileOutputStream(getDataPathForKey(bucket, key), false).use {
                data.copyTo(it, 16 * 1024)
            }
            File(getMetaPathForKey(bucket, key)).writeText(meta.toJson())
            return@lockObjectForWrite PutResult(bucket, key, size, meta.contentType)
        }
    }

    fun put(bucket: String, key: String, data: ByteArray, meta: MetaData) =
            put(bucket, key, ByteArrayInputStream(data), meta)

    override fun append(bucket: String, key: String, data: InputStream,
                        meta: MetaData) = withBucket(
            bucket) {
        val file = File(getDataPathForKey(bucket, key))
        if (!file.exists())
            throw KeyNotFoundException(bucket, key)

        return@withBucket lockObjectForWrite(bucket, key) {
            val size = FileOutputStream(file, true).use {
                data.copyTo(it, 16 * 1024)
            }

            // Update meta data
            if (meta.other.isNotEmpty()) {
                val oldMeta = fromJson<MetaData>(File(getMetaPathForKey(bucket, key)).readText())
                meta.other.forEach {
                    oldMeta.set(it.key, it.value)
                }
                File(getMetaPathForKey(bucket, key)).writeText(oldMeta.toJson())
            }

            return@lockObjectForWrite PutResult(bucket, key, size, meta.contentType)
        }
    }

    override fun getMeta(bucket: String, key: String): MetaData = withBucket(bucket) {
        return@withBucket try {
            val meta = fromJson<MetaData>(File(getMetaPathForKey(bucket, key)).readText())
            val file = File(getDataPathForKey(bucket, key))
            val attr = java.nio.file.Files.readAttributes(file.toPath(),
                                                          BasicFileAttributes::class.java)

            meta.lastModified = attr.lastModifiedTime().toMillis()
            meta.objectSize = attr.size()
            return@withBucket meta
        } catch (e: FileNotFoundException) {
            throw KeyNotFoundException(bucket, key)
        }
    }

    override fun get(bucket: String, key: String, start: Long?,
                     end: Long?): GetResult = withBucket(bucket) {
        // Check for invalid ranges
        if (start != null && start < 0)
            throw InvalidRangeRequest(
                    "start: $start and end: $end")
        if (end != null && end < 0)
            throw InvalidRangeRequest(
                    "start: $start and end: $end")
        if (start == null && end != null)
            throw InvalidRangeRequest(
                    "start: $start and end: $end")

        try {
            val file = File(getDataPathForKey(bucket, key))

            val actualStart = start ?: 0
            val fileSize = file.length()
            val actualEnd = end ?: fileSize - 1
            val responseLength = actualEnd - actualStart + 1
            if (responseLength < 0)
                throw InvalidRangeRequest(
                        "start: $start and end: $end")

            val storedMeta = getMeta(bucket, key)
            storedMeta.contentLength = responseLength

            val fileStream = file.inputStream()
            fileStream.channel.position(actualStart)
            val stream = RangeStream(fileStream, responseLength)

            return@withBucket GetResult(bucket, key, stream, storedMeta)
        } catch (e: FileNotFoundException) {
            throw KeyNotFoundException(bucket, key)
        }
    }

    override fun delete(bucket: String, key: String) = withBucket(bucket) {
        lockObjectForWrite(bucket, key) {
            if (!File(getDataPathForKey(bucket, key)).delete())
                throw KeyNotFoundException(bucket, key)
            File(getMetaPathForKey(bucket, key)).delete()
        }
        return@withBucket
    }

    private fun getDataPathForKey(bucket: String, key: String): String =
            "${DirectoryCalculator.getTwoNestedLevels(rootDir, key)}.$bucket.object"

    private fun getMetaPathForKey(bucket: String, key: String): String =
            "${DirectoryCalculator.getTwoNestedLevels(rootDir, key)}.$bucket.meta"

    private fun <R> withBucket(bucket: String, block: () -> R): R {
        val bucketInfo = getBucketInfo(bucket)
        if (!bucketInfo.isPresent)
            throw BucketNotFound(bucket)
        return bucketInfo.get().lock.read {
            block.invoke()
        }
    }

    private fun <R> lockObjectForWrite(bucket: String, key: String, block: () -> R): R {
        val lock = lockCache.get("$bucket:$key") { ReentrantReadWriteLock() }
        return lock.write(block)
    }

}