package com.github.shahrivari.restorage

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.shahrivari.restorage.store.fs.FileSystemStore
import io.javalin.Javalin
import mu.KotlinLogging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.util.thread.QueuedThreadPool

class ReStorageApp : CliktCommand() {
    private val port: Int by option(help = "Port to listen").int().default(7000)
    private val threads: Int by option(help = "Executor threads").int().default(256)
    private val root: String by option(help = "Root directory of storage").default("/dev/shm/store")
    private lateinit var app: Javalin
    private val logger = KotlinLogging.logger {}

    override fun run() {
        runOnPort(port, root, threads)
    }

    fun runOnPort(port: Int, root: String, threads: Int = 256): ReStorageApp {
        logger.info { "Starting app on port: $port, dir: $root" }
        app = Javalin.create { config ->
            config.server {
                Server(QueuedThreadPool(threads))
            }
        }
        val store = FileSystemStore(root)
        val controller = Controller(app, store)
        controller.wireUp()
        app = app.start(port)
        return this
    }

    fun stop() {
        app.stop()
    }
}

fun main(args: Array<String>) {
    ReStorageApp().main(args)
}
