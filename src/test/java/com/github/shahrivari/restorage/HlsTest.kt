package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.store.Store
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.*
import java.io.File
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HlsTest {
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

    @BeforeEach
    private fun resetDefaultBucket() {
        val defaultKey = Random().nextLong().toString()
        if (client.bucketNotExists(DEFAULT_BUCKET))
            client.createBucket(DEFAULT_BUCKET)
        if (client.objectExists(DEFAULT_BUCKET, defaultKey))
            client.deleteObject(DEFAULT_BUCKET, defaultKey)
    }

    @Test
    fun `test hls generation`() {
        val key = Random().nextLong().toString()
        val mp4 = requireNotNull(HlsTest::class.java.getResource("/sample.mp4")?.readBytes())
        val put = client.putObject(DEFAULT_BUCKET, key, mp4)
        val hls = client.generateHls(DEFAULT_BUCKET, key)
        val file = client.getHlsFile(DEFAULT_BUCKET, key, Store.M3U8_FILE_NAME)
        Assertions.assertThat(file).isNotNull()
        val text = String(file!!, Charsets.UTF_8)
        Assertions.assertThat(text).startsWith("#EXTM3U")
    }
}