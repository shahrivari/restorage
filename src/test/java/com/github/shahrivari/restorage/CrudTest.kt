package com.github.shahrivari.restorage

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CrudTest : BaseReStorageTest() {
    val defaultKey = "1"
    val defaultValue = "123456789".toByteArray()

    @BeforeEach
    private fun resetDefaultBucket() {
        if (!client.bucketExists(DEFAULT_BUCKET))
            client.createBucket(DEFAULT_BUCKET)
    }


    @Test
    fun `test simple put`() {
        client.putObject(DEFAULT_BUCKET, defaultKey, defaultValue)
        val result = client.getObject(DEFAULT_BUCKET, defaultKey)
        assertTrue { result?.stream?.readBytes()?.contentEquals(defaultValue) ?: false  }
    }
}