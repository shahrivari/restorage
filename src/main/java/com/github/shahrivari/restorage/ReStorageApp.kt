package com.github.shahrivari.restorage

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.shahrivari.restorage.exception.ReStorageException
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
        val controller = Controller(store)
        wireUp(controller)
        app = app.start(port)
        return this
    }

    fun stop() {
        app.stop()
    }

    private fun wireUp(controller: Controller) {
        app.post(Controller.BUCKET_CRUD_PATH) { ctx -> controller.createBucket(ctx) }

        app.get(Controller.BUCKET_CRUD_PATH) { ctx -> controller.getBucketInfo(ctx) }

        app.head(Controller.BUCKET_CRUD_PATH) { ctx -> controller.headBucket(ctx) }

        app.delete(Controller.BUCKET_CRUD_PATH) { ctx -> controller.deleteBucket(ctx) }

        app.post(Controller.OBJECT_CRUD_PATH) { ctx -> controller.putObject(ctx) }

        app.put(Controller.OBJECT_CRUD_PATH) { ctx -> controller.putChunk(ctx) }

        app.get(Controller.OBJECT_CRUD_PATH) { ctx -> controller.getObject(ctx) }

        app.get("${Controller.OBJECT_CRUD_PATH}/meta") { ctx -> controller.getObjectMeta(ctx) }

        app.get("${Controller.OBJECT_CRUD_PATH}/md5") { ctx -> controller.getMd5(ctx) }

        app.head(Controller.OBJECT_CRUD_PATH) { ctx -> controller.headObject(ctx) }

        app.delete(Controller.OBJECT_CRUD_PATH) { ctx -> controller.deleteObject(ctx) }


        app.exception(ReStorageException::class.java) { e, ctx ->
            ctx.status(e.statusCode)
            ctx.json(mapOf("errorCode" to e.errorCode, "message" to e.message))
        }

        app.after { ctx ->
            ctx.res.setHeader("Server", "ReStorage")
        }

    }
}

fun main(args: Array<String>) {
    ReStorageApp().main(args)
}
