package com.github.shahrivari.restorage.store

import com.github.shahrivari.restorage.*
import com.github.shahrivari.restorage.commons.RangeStream
import com.github.shahrivari.restorage.commons.toJson
import com.google.common.hash.Hashing
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import java.io.*

data class KeyMetaData(val bucket: String,
                       val key: String,
                       val contentType: String?)

class FileSystemBasedStore(val rootDir: String) {
    private val bucketMetaDataStore = BucketMetaDataStore(rootDir)

    init {
        val sep = File.separator

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

    fun bucketExists(bucket: String): Boolean =
            bucketMetaDataStore.bucketExists(bucket)

    fun deleteBucket(bucket: String) =
            bucketMetaDataStore.deleteBucket(bucket)

    fun put(bucket: String, key: String, data: InputStream, contentType: String?) = withBucket(bucket) {
        FileOutputStream(getDataPathForKey(bucket, key), false).use {
            ByteStreams.copy(data, it)
        }
        File(getMetaPathForKey(bucket, key)).writeText(
                KeyMetaData(bucket, key, contentType).toJson()
        )
    }

    fun append(bucket: String, key: String, data: InputStream) = withBucket(bucket) {
        val file = File(getDataPathForKey(bucket, key))
        if (!file.exists())
            throw KeyNotFoundException(bucket, key)
        FileOutputStream(file, true).use {
            ByteStreams.copy(data, it)
        }
    }

    fun computeMd5(bucket: String, key: String): String {
        val source = Files.asByteSource(File(getDataPathForKey(bucket, key)))
        return source.hash(Hashing.md5()).toString().toLowerCase()
    }

    fun get(bucket: String, key: String) = withBucket(bucket) {
        try {
            return@withBucket FileInputStream(getDataPathForKey(bucket, key))
        } catch (e: FileNotFoundException) {
            throw KeyNotFoundException(bucket, key)
        }
    }

    fun objectExists(bucket: String, key: String) = withBucket(bucket) {
        return@withBucket try {
            File(getDataPathForKey(bucket, key)).exists()
        } catch (e: FileNotFoundException) {
            false
        }
    }

    fun getRange(bucket: String, key: String, offset: Long, size: Long) = withBucket(bucket) {
        try {
            val stream =
                    RangeStream.getRangeStreamFromFile(getDataPathForKey(bucket, key), offset, size)
            return@withBucket RangeResponse(offset, offset + size, RangeStream(stream, size))
        } catch (e: FileNotFoundException) {
            throw KeyNotFoundException(bucket, key)
        }
    }

    fun delete(bucket: String, key: String) = withBucket(bucket) {
        deleteFileIfExists(getDataPathForKey(bucket, key))
        deleteFileIfExists(getMetaPathForKey(bucket, key))
        return@withBucket
    }

    private fun deleteFileIfExists(path: String) {
        val file = File(path)
        if (file.exists())
            file.delete()
    }

    private fun <R> withBucket(bucket: String, block: () -> R): R {
        if (!bucketExists(bucket))
            throw BucketNotFound(bucket)
        return block()
    }
}