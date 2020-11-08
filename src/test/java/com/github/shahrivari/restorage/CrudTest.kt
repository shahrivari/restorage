package com.github.shahrivari.restorage

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        assertTrue { result?.stream?.readBytes()?.contentEquals(defaultValue) ?: false }
    }

    @Test
    fun `test range request`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey, 10, 20)
        val range = defaultValue.sliceArray(IntRange(10, 20))
        assertTrue { result?.stream?.readBytes()?.contentEquals(range) ?: false }
    }
}