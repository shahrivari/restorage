package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.InvalidRangeRequest
import com.github.shahrivari.restorage.KeyNotFoundException
import com.github.shahrivari.restorage.commons.RangeStream
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.hash.Hashing
import com.google.common.io.ByteStreams
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.InputStream
import java.nio.file.attribute.BasicFileAttributes
import java.util.*


class FileSystemBasedStore(val rootDir: String) {
    private val bucketMetaDataStore = BucketMetaDataStore(rootDir)

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

    private fun getDataPathForKey(bucket: String, key: String): String {
        val sha2 = Hashing.sha256().hashString(key, Charsets.UTF_8).toString()
        val sep = File.separator
        return "$rootDir$sep${sha2.substring(0, 2)}$sep${sha2.substring(2, 4)}$sep${sha2.substring(4)}.$bucket"
    }

    private fun getMetaPathForKey(bucket: String, key: String): String =
            getDataPathForKey(bucket, key) + ".meta"

    fun createBucket(bucket: String) =
            bucketMetaDataStore.createBucket(bucket)

    fun getBucketInfo(bucket: String): Optional<BucketInfo> =
            bucketMetaDataStore.getBucketInfo(bucket)

    fun deleteBucket(bucket: String) =
            bucketMetaDataStore.deleteBucket(bucket)

    fun put(bucket: String, key: String, data: InputStream, meta: MetaData) = withBucket(bucket) {
        val size = FileOutputStream(getDataPathForKey(bucket, key), false).use {
            data.copyTo(it, 16 * 1024)
        }
        File(getMetaPathForKey(bucket, key)).writeText(meta.toJson())
        return@withBucket PutResult(bucket, key, size, meta.contentType)
    }

    fun append(bucket: String, key: String, data: InputStream, meta: MetaData) = withBucket(bucket) {
        val file = File(getDataPathForKey(bucket, key))
        if (!file.exists())
            throw KeyNotFoundException(bucket, key)
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

        return@withBucket PutResult(bucket, key, size, meta.contentType)
    }

    fun getMeta(bucket: String, key: String): MetaData = withBucket(bucket) {
        return@withBucket try {
            val meta = fromJson<MetaData>(File(getMetaPathForKey(bucket, key)).readText())
            val file = File(getDataPathForKey(bucket, key))
            val attr = java.nio.file.Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)

            meta.lastModified = attr.lastModifiedTime().toMillis()
            meta.objectSize = attr.size()
            return@withBucket meta
        } catch (e: FileNotFoundException) {
            throw KeyNotFoundException(bucket, key)
        }
    }

    fun get(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult = withBucket(bucket) {
        // Check for invalid ranges
        if (start != null && start < 0)
            throw InvalidRangeRequest("start: $start and end: $end")
        if (end != null && end < 0)
            throw InvalidRangeRequest("start: $start and end: $end")
        if (start == null && end != null)
            throw InvalidRangeRequest("start: $start and end: $end")

        try {
            val file = File(getDataPathForKey(bucket, key))

            val actualStart = start ?: 0
            val fileSize = file.length()
            val actualEnd = end ?: fileSize - 1
            val responseLength = actualEnd - actualStart + 1
            if (responseLength < 0)
                throw InvalidRangeRequest("start: $start and end: $end")

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

    fun delete(bucket: String, key: String) = withBucket(bucket) {
        if (!File(getDataPathForKey(bucket, key)).delete())
            throw KeyNotFoundException(bucket, key)
        File(getMetaPathForKey(bucket, key)).delete()
        return@withBucket
    }

    private fun <R> withBucket(bucket: String, block: () -> R): R {
        if (!getBucketInfo(bucket).isPresent)
            throw BucketNotFound(bucket)
        return block()
    }
}