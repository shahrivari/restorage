package com.github.shahrivari.restorage

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.store.FileSystemBasedStore
import io.javalin.Javalin
import java.io.FileInputStream

class ReStorageApp : CliktCommand() {
    private val port: Int by option(help = "Port to listen").int().default(7000)
    private val root: String by option(help = "Root directory of storage").default("/dev/shm/store")
    private lateinit var app: Javalin

    override fun run() {
        runOnPort(port, root)
    }

    fun runOnPort(port: Int, root: String): ReStorageApp {
        app = Javalin.create()
        val store = FileSystemBasedStore(root)
        val controller = Controller(app, store)
        controller.wireUp()
        app.start(port)
        return this
    }

    fun stop() {
        app.stop()
    }
}

fun main(args: Array<String>) {
    ReStorageApp().main(args)
    Thread.sleep(500)
    val client = ReStorageClient("http://localhost:7000")
    if (client.bucketExists("alaki"))
        client.deleteBucket("alaki")
    val res4 = client.createBucket("alaki")
//    val res5 = client.putObject("alaki", "1", FileInputStream("/home/reza/Downloads/ubuntu-16.04.6-server-arm64.iso"))
    val res6 = client.getObject("alaki", "1")
    println()
}

//TODO: check buckets name
//TODO: lock buckets and keys