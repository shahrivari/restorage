package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.commons.randomBytes
import com.github.shahrivari.restorage.exception.BucketNotFoundException
import com.github.shahrivari.restorage.exception.KeyNotFoundException
import com.github.shahrivari.restorage.exception.MetaDataTooLargeException
import com.github.shahrivari.restorage.store.fs.FileSystemStore
import com.google.common.hash.Hashing
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.*
import java.io.ByteArrayInputStream
import java.io.File
import java.util.*
import kotlin.concurrent.thread

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CrudTest {
    companion object {
        const val port = 8000
        const val dir = "/dev/shm/restorage_unit_tests"
        lateinit var app: ReStorageApp
        const val DEFAULT_BUCKET = "alaki"

        val client by lazy { ReStorageClient("http://localhost:$port") }

    }

    @BeforeAll
    fun startServer() {
        File(dir).deleteRecursively()
        app = ReStorageApp().runOnPort(port, dir)
        Runtime.getRuntime().addShutdownHook(Thread { app.stop() })
    }

    @AfterAll
    fun dispose() {
        File(dir).deleteRecursively()
        app.stop()
    }


    val defaultKey = "1"
    val defaultValue = "0123456789".repeat(100).toByteArray()

    @BeforeEach
    private fun resetDefaultBucket() {
        if (client.bucketNotExists(DEFAULT_BUCKET))
            client.createBucket(DEFAULT_BUCKET)
        if (client.objectExists(DEFAULT_BUCKET, defaultKey))
            client.deleteObject(DEFAULT_BUCKET, defaultKey)
    }

    @Test
    fun `test simple put`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test empty put`() {
        val emptyBytes = ByteArray(0)
        client.putObject(DEFAULT_BUCKET, defaultKey, emptyBytes)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(emptyBytes).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test simple append`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        client.appendObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue + defaultValue).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test range request`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey, 10, 20)
        val range = defaultValue.sliceArray(IntRange(10, 20))
        Assertions.assertThat(range).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test big object`() {
        val bigValue = ByteArray(10_000_000)
        Random().nextBytes(bigValue)
        client.putObject(DEFAULT_BUCKET, defaultKey, bigValue)

        var result = client.getObject(DEFAULT_BUCKET, defaultKey)?.getAllBytes()

        Assertions.assertThat(Arrays.equals(bigValue, result)).isTrue()

        result = client.getObject(DEFAULT_BUCKET, defaultKey, 10, 20)?.getAllBytes()
        Assertions.assertThat(bigValue.sliceArray(IntRange(10, 20))).isEqualTo(result)

        result = client.getObject(DEFAULT_BUCKET, defaultKey, 10000, 20000)?.getAllBytes()
        Assertions.assertThat(bigValue.sliceArray(IntRange(10000, 20000))).isEqualTo(result)
    }

    @Test
    fun `test put with meta data`() {
        val meta = mapOf("meta1" to "1", "meta2" to "2")
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue, metaData = meta)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
    }

    @Test
    fun `should fail on large meta data`() {
        val meta = mapOf("meta1" to "1", "meta2" to "2".repeat(FileSystemStore.MAX_META_SIZE + 9))

        Assertions.assertThatThrownBy {
            client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue, metaData = meta)
        }.isInstanceOf(MetaDataTooLargeException::class.java)
    }

    @Test
    fun `test append with meta data`() {
        val meta = mutableMapOf("meta1" to "1", "meta2" to "2")
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue, metaData = meta)
        var result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
        meta["meta3"] = "3"
        client.appendObject(DEFAULT_BUCKET, defaultKey, defaultValue, metaData = meta)
        result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue + defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
    }

    @Test
    fun `test delete object`() {
        Assertions.assertThatThrownBy {
            client.deleteObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFoundException::class.java)

        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val deleteResponse = client.deleteObject(DEFAULT_BUCKET, defaultKey)

        Assertions.assertThat(defaultValue.size).isEqualTo(deleteResponse?.size ?: 0)

        Assertions.assertThatThrownBy {
            client.deleteObject(DEFAULT_BUCKET, defaultKey)
        }.isInstanceOf(KeyNotFoundException::class.java)

        Assertions.assertThatThrownBy {
            client.deleteObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFoundException::class.java)
    }

    @Test
    fun `test get with invalid bucket`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        Assertions.assertThatThrownBy {
            client.getObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFoundException::class.java)
    }

    @Test
    fun `test put with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.putObject("INVALID_BUCKET", defaultKey, defaultValue)
        }.isInstanceOf(BucketNotFoundException::class.java)
    }

    @Test
    fun `test append with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.appendObject("INVALID_BUCKET", defaultKey, defaultValue)
        }.isInstanceOf(BucketNotFoundException::class.java)
    }

    @Test
    fun `test get with invalid key`() {
        Assertions.assertThatThrownBy {
            client.getObject(DEFAULT_BUCKET, defaultKey)
        }.isInstanceOf(KeyNotFoundException::class.java)
    }

    @Test
    fun `store empty file with some meta`() {
        val meta = mutableMapOf("meta1" to "1", "meta2" to "2")
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(ByteArray(0)).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
    }

    @Test
    fun `test meta data manipulation`() {
        val meta = mutableMapOf("meta1" to "1", "meta2" to "2")
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        var metaData = client.getObjectMeta(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(meta).isEqualTo(metaData?.other)

        meta["meta3"] = "3"
        client.appendObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        metaData = client.getObjectMeta(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(meta).isEqualTo(metaData?.other)

        meta["meta2"] = "3"
        client.appendObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        metaData = client.getObjectMeta(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(meta).isEqualTo(metaData?.other)

        meta["meta2"] = ""
        client.appendObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        var result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
        Assertions.assertThat(result?.getAllBytes()).isEqualTo(ByteArray(0))

        meta.clear()
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArray(0), metaData = meta)
        result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
        Assertions.assertThat(result?.getAllBytes()).isEqualTo(ByteArray(0))
    }

    @Test
    fun `test concurrent put`() {
        val bigValue = randomBytes(10_000_000)
        val thread = thread {
            client.putObject(DEFAULT_BUCKET, defaultKey, bigValue)
        }
        val smallValue = randomBytes(5000)
        client.putObject(DEFAULT_BUCKET, defaultKey, smallValue)
        thread.join()
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)?.getAllBytes()
        println(result?.size)
        val any = listOf(bigValue, smallValue).any { Arrays.equals(it, result) }
        Assertions.assertThat(any).isTrue
    }

    @Test
    fun `test multi concurrent put`() {
        val values = (0..40).map { randomBytes(1_000_000) }.toList()
        val threads = values.map { thread { client.putObject(DEFAULT_BUCKET, defaultKey, it) } }
        threads.forEach { it.join() }
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)?.getAllBytes()
        Assertions.assertThat(values.any { Arrays.equals(it, result) }).isTrue()
    }

    @Test
    fun `get after bucket delete should fail`() {
        val bucket = "reza"
        val key = "key"
        if (!client.bucketExists(bucket))
            client.createBucket(bucket)
        client.putObject(bucket, key, "Some value".toByteArray())
        client.getObject(bucket, key)
        client.deleteBucket(bucket)
        client.createBucket(bucket)
        Assertions.assertThatThrownBy {
            client.getObject(bucket, key)
        }.isInstanceOf(KeyNotFoundException::class.java)
    }

    @Test
    fun `get md5 calculation`() {
        val bucket = "reza"
        val key = "key"
        val value = "Some value"
        if (!client.bucketExists(bucket))
            client.createBucket(bucket)
        client.putObject(bucket, key, value.toByteArray())
        val meta = client.getObjectMd5(bucket, key)
        val md5 = Hashing.md5().hashString(value, Charsets.UTF_8).toString()
        Assertions.assertThat(meta?.get("md5")).isEqualTo(md5)
    }
}