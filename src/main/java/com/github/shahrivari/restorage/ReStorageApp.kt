package com.github.shahrivari.restorage

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.shahrivari.restorage.store.FileSystemBasedStore
import io.javalin.Javalin

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

fun main(args: Array<String>) = ReStorageApp().main(args)

//TODO: check buckets name
//TODO: lock buckets and keys