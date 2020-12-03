package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.exception.BucketAlreadyExistsException
import com.github.shahrivari.restorage.exception.BucketNotFoundException
import org.junit.jupiter.api.*
import java.io.File
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BucketTest {

    companion object {
        const val port = 8000
        const val dir = "/dev/shm/restorage_unit_tests"
        lateinit var app: ReStorageApp
        const val DEFAULT_BUCKET = "alaki"

        val client by lazy { ReStorageClient("http://localhost:$port") }

    }

    @BeforeAll
    fun startServer() {
        File(CrudTest.dir).deleteRecursively()
        CrudTest.app = ReStorageApp().runOnPort(CrudTest.port, CrudTest.dir)
        Runtime.getRuntime().addShutdownHook(Thread { CrudTest.app.stop() })
    }

    @AfterAll
    fun dispose() {
        File(CrudTest.dir).deleteRecursively()
        CrudTest.app.stop()
    }


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
        assertThrows<BucketAlreadyExistsException> { client.createBucket(DEFAULT_BUCKET) }
    }

    @Test
    fun `test get bucket`() {
        assertThrows<BucketNotFoundException> { client.getBucket("INVALID_BUCKET") }
        client.createBucket(DEFAULT_BUCKET)
        client.getBucket(DEFAULT_BUCKET)
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
        assertThrows<BucketNotFoundException> { client.deleteBucket("ABSENT_BUCKET") }
    }
}