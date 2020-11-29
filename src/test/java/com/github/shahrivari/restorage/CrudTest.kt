package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.commons.randomBytes
import com.github.shahrivari.restorage.exception.BucketNotFound
import com.github.shahrivari.restorage.exception.KeyNotFoundException
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
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(bigValue))

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
        }.isInstanceOf(BucketNotFound::class.java)

        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        client.deleteObject(DEFAULT_BUCKET, defaultKey)

        Assertions.assertThatThrownBy {
            client.deleteObject(DEFAULT_BUCKET, defaultKey)
        }.isInstanceOf(KeyNotFoundException::class.java)

        Assertions.assertThatThrownBy {
            client.deleteObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test get with invalid bucket`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        Assertions.assertThatThrownBy {
            client.getObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test put with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.putObject("INVALID_BUCKET", defaultKey, defaultValue)
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test append with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.appendObject("INVALID_BUCKET", defaultKey, defaultValue)
        }.isInstanceOf(BucketNotFound::class.java)
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
        Assertions.assertThat(
                listOf(bigValue, smallValue).any { Arrays.equals(it, result) }).isTrue()
    }

    @Test
    fun `test multi concurrent put`() {
        val values = (0..40).map { randomBytes(1_000_000) }.toList()
        val threads = values.map { thread { client.putObject(DEFAULT_BUCKET, defaultKey, it) } }
        threads.forEach { it.join() }
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)?.getAllBytes()
        Assertions.assertThat(values.any { Arrays.equals(it, result) }).isTrue()
    }

}