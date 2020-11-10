package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.client.ReStorageClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import java.io.File

open class BaseReStorageTest {

    companion object {
        const val port = 8000
        const val dir = "/dev/shm/restorage_unit_tests"
        lateinit var app: ReStorageApp
        const val DEFAULT_BUCKET = "alaki"

        val client by lazy { ReStorageClient("http://localhost:$port") }

        @BeforeAll
        @JvmStatic
        fun startServer() {
            File(dir).deleteRecursively()
            app = ReStorageApp().runOnPort(port, dir)
            Runtime.getRuntime().addShutdownHook(Thread { app.stop() })
        }

        @AfterAll
        @JvmStatic
        fun dispose() {
            File(dir).deleteRecursively()
            app.stop()
        }
    }

}