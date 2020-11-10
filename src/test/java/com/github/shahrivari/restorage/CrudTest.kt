package com.github.shahrivari.restorage

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.util.*

class CrudTest : BaseReStorageTest() {
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
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test simple append`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
        client.appendObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue + defaultValue).isEqualTo(result?.getAllBytes())
    }

    @Test
    fun `test range request`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
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
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue), metaData = meta)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
    }

    @Test
    fun `test append with meta data`() {
        val meta = mutableMapOf("meta1" to "1", "meta2" to "2")
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue), metaData = meta)
        var result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
        meta["meta3"] = "3"
        client.appendObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue), metaData = meta)
        result = client.getObject(DEFAULT_BUCKET, defaultKey)
        Assertions.assertThat(defaultValue + defaultValue).isEqualTo(result?.getAllBytes())
        Assertions.assertThat(meta).isEqualTo(result?.metaData?.other)
    }

    @Test
    fun `test delete object`() {
        Assertions.assertThatThrownBy {
            client.deleteObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFound::class.java)

        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
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
        client.putObject(DEFAULT_BUCKET, defaultKey, ByteArrayInputStream(defaultValue))
        Assertions.assertThatThrownBy {
            client.getObject("INVALID_BUCKET", defaultKey)
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test put with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.putObject("INVALID_BUCKET", defaultKey, ByteArrayInputStream(defaultValue))
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test append with invalid bucket`() {
        Assertions.assertThatThrownBy {
            client.appendObject("INVALID_BUCKET", defaultKey, ByteArrayInputStream(defaultValue))
        }.isInstanceOf(BucketNotFound::class.java)
    }

    @Test
    fun `test get with invalid key`() {
        Assertions.assertThatThrownBy {
            client.getObject(DEFAULT_BUCKET, defaultKey)
        }.isInstanceOf(KeyNotFoundException::class.java)
    }


}