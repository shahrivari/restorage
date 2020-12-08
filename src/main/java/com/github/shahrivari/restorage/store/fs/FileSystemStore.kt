package com.github.shahrivari.restorage.store.fs

import com.github.shahrivari.restorage.commons.*
import com.github.shahrivari.restorage.exception.BucketNotFoundException
import com.github.shahrivari.restorage.exception.InvalidRangeRequestException
import com.github.shahrivari.restorage.exception.KeyNotFoundException
import com.github.shahrivari.restorage.exception.MetaDataTooLargeException
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
    private val bucketMetaDataStore: BucketMetaDataStore

    private val lockCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.SECONDS)
            .build<String, ReentrantReadWriteLock>()

    init {
        val sep = File.separator
        File(rootDir).mkdirs()
        bucketMetaDataStore = BucketMetaDataStore(rootDir)
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

    companion object {
        const val MAX_META_SIZE = 1024
    }

    override fun createBucket(bucket: String) =
            bucketMetaDataStore.createBucket(bucket)

    override fun getBucketInfo(bucket: String): Optional<BucketInfo> =
            bucketMetaDataStore.getBucket(bucket)

    override fun deleteBucket(bucket: String) =
            bucketMetaDataStore.deleteBucket(bucket)

    private fun makeMeta(meta: MetaData): ByteArray {
        val stream = ByteArrayOutputStream(MAX_META_SIZE)
        stream.bufferedWriter(Charsets.UTF_8).use {
            it.write(meta.toJson())
            it.newLine()
            it.flush()
            if ((stream.size() < MAX_META_SIZE - 1)) {
                val padding = CharArray(MAX_META_SIZE - stream.size()) { ' ' }
                padding[padding.size - 1] = '\n'
                it.write(padding)
            }
        }
        if (stream.size() > MAX_META_SIZE)
            throw MetaDataTooLargeException(meta.bucket, meta.key)
        return stream.toByteArray()
    }

    override fun put(bucket: String, key: String, data: InputStream, meta: MetaData) =
            withBucket(bucket) {
                return@withBucket lockObjectForWrite(bucket, key) {
                    val size = RandomAccessFile(getFilePathForKey(bucket, key), "rw").use { file ->
                        file.setLength(0)
                        file.write(makeMeta(meta))
                        return@use data.copyTo(file)
                    }
                    return@lockObjectForWrite PutResult(bucket, key, size, meta.contentType)
                }
            }

    fun put(bucket: String, key: String, data: ByteArray, meta: MetaData) =
            put(bucket, key, ByteArrayInputStream(data), meta)

    override fun append(bucket: String, key: String, data: InputStream, meta: MetaData) =
            withBucket(bucket) {
                val file = File(getFilePathForKey(bucket, key))
                if (!file.exists())
                    throw KeyNotFoundException(bucket, key)

                return@withBucket lockObjectForWrite(bucket, key) {
                    val size = RandomAccessFile(file, "rw").use { randomFile ->
                        if (meta.other.isNotEmpty()) {
                            randomFile.seek(0)
                            val metaBytes = ByteArray(MAX_META_SIZE)
                            randomFile.read(metaBytes)
                            val oldMeta = fromJson<MetaData>(String(metaBytes, Charsets.UTF_8))
                            meta.other.forEach {
                                oldMeta.set(it.key, it.value)
                            }
                            randomFile.seek(0)
                            randomFile.write(makeMeta(meta))
                        }
                        randomFile.seek(randomFile.length())
                        return@use data.copyTo(randomFile)
                    }

                    return@lockObjectForWrite PutResult(bucket, key, size, meta.contentType)
                }
            }

    override fun getMeta(bucket: String, key: String): MetaData = withBucket(bucket) {
        return@withBucket try {
            //val meta = fromJson<MetaData>(File(getMetaPathForKey(bucket, key)).readText())
            val file = File(getFilePathForKey(bucket, key))
            val meta = RandomAccessFile(file, "r").use { randomFile ->
                randomFile.seek(0)
                val metaBytes = ByteArray(MAX_META_SIZE)
                randomFile.read(metaBytes)
                return@use fromJson<MetaData>(String(metaBytes, Charsets.UTF_8))
            }

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
            throw InvalidRangeRequestException(
                    "start: $start and end: $end")
        if (end != null && end < 0)
            throw InvalidRangeRequestException(
                    "start: $start and end: $end")
        if (start == null && end != null)
            throw InvalidRangeRequestException(
                    "start: $start and end: $end")

        try {
            val file = File(getFilePathForKey(bucket, key))
            if (!file.exists())
                throw KeyNotFoundException(bucket, key)

            val actualStart = (start ?: 0) + MAX_META_SIZE
            val actualEnd = end?.plus(MAX_META_SIZE) ?: file.length() - 1
            val responseLength = actualEnd - actualStart + 1
            if (responseLength < 0)
                throw InvalidRangeRequestException("start: $start and end: $end")

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
            if (!File(getFilePathForKey(bucket, key)).delete())
                throw KeyNotFoundException(bucket, key)
        }
        return@withBucket
    }

    private fun getFilePathForKey(bucket: String, key: String): String {
        val bucketInfo = getBucketInfo(bucket)
        if (!bucketInfo.isPresent)
            throw BucketNotFoundException(bucket)
        val path = DirectoryCalculator.getTwoNestedLevels(rootDir, key)
        return "$path.${bucketInfo.get().id}.object"
    }

    private fun <R> withBucket(bucket: String, block: () -> R): R {
        val bucketInfo = getBucketInfo(bucket)
        if (!bucketInfo.isPresent)
            throw BucketNotFoundException(bucket)
        return bucketInfo.get().lock.read {
            block.invoke()
        }
    }

    private fun <R> lockObjectForWrite(bucket: String, key: String, block: () -> R): R {
        val lock = lockCache.get("$bucket:$key") { ReentrantReadWriteLock() }
        return lock.write(block)
    }

    private fun InputStream.copyTo(out: RandomAccessFile, bufferSize: Int = 16 * 1024): Long {
        var bytesCopied: Long = 0
        val buffer = ByteArray(bufferSize)
        var bytes = read(buffer)
        while (bytes >= 0) {
            out.write(buffer, 0, bytes)
            bytesCopied += bytes
            bytes = read(buffer)
        }
        return bytesCopied
    }

}