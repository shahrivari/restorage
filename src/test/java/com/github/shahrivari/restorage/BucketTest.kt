package com.github.shahrivari.restorage

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BucketTest : BaseReStorageTest() {
    @BeforeEach
    private fun resetDefaultBucket() {
        if (client.bucketExists(DEFAULT_BUCKET))
            client.deleteBucket(DEFAULT_BUCKET)
    }


    @Test
    fun `test bucket creation`() {
        assertFalse { client.bucketExists(DEFAULT_BUCKET) }
        client.createBucket(DEFAULT_BUCKET)
        assertTrue { client.bucketExists(DEFAULT_BUCKET) }
    }


    @Test
    fun `should fail on duplicate bucket`() {
        client.createBucket(DEFAULT_BUCKET)
        assertThrows<BucketAlreadyExists> { client.createBucket(DEFAULT_BUCKET) }
    }

    @Test
    fun `should fail on existence of absent bucket`() {
        assertFalse { client.bucketExists(DEFAULT_BUCKET) }
    }

    @Test
    fun `test bucket deletion`() {
        client.createBucket(DEFAULT_BUCKET)
        client.deleteBucket(DEFAULT_BUCKET)
    }

    @Test
    fun `should fail on absent bucket deletion`() {
        assertThrows<BucketNotFound> { client.deleteBucket("ABSENT_BUCKET") }
    }
}