package com.github.shahrivari.restorage

import io.restassured.RestAssured
import io.restassured.RestAssured.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File

open class ReStorageTest {

    companion object {
        const val port = 8000
        const val dir = "/dev/shm/test11"
        lateinit var app: ReStorageApp
        const val DEFAULT_BUCKET = "alaki"

        const val CREATE_DEFAULT_BUCKET_PATH = "/bucket/create/$DEFAULT_BUCKET"
        const val EXISTS_DEFAULT_BUCKET_PATH = "/bucket/exists/$DEFAULT_BUCKET"
        const val DELETE_DEFAULT_BUCKET_PATH = "/bucket/delete/$DEFAULT_BUCKET"


        @BeforeAll
        @JvmStatic
        fun startServer() {
            File(dir).deleteRecursively()
            app = ReStorageApp().runOnPort(port, dir)
            baseURI = "http://localhost"
            RestAssured.port = port
            Runtime.getRuntime().addShutdownHook(Thread { app.stop() })
        }
    }
}