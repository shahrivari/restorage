package com.github.shahrivari.restorage.bench

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.shahrivari.restorage.ReStorageApp
import com.github.shahrivari.restorage.client.ReStorageClient
import com.github.shahrivari.restorage.commons.randomBytes
import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.RateLimiter
import mu.KotlinLogging
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class Benchmark : CliktCommand() {
    private val logger = KotlinLogging.logger {}
    private val port: Int by option(help = "Port to listen").int().default(7000)
    private val benchmarkThreads: Int by option(help = "Benchmark threads").int().default(100)
    private val serverThreads: Int by option(help = "Server threads").int().default(256)
    private val root: String by option(help = "Root directory of storage").default("/tmp/benchmark")
    private val requests: Int by option(help = "Number of requests").int().default(20_000)
    private val valueSize: Int by option(help = "Size of value").int().default(8 * 1024)
    private val reportInterval: Int by option(help = "Steps of report").int().default(1000)
    private val rate: Int by option(help = "Rate of insert").int().default(10000)

    override fun run() {
        val bucket = "test"
        logger.info { "Cleaning directory: $root recursively...." }
        File(root).deleteRecursively()
        val app = ReStorageApp().runOnPort(port, root, serverThreads)
        Runtime.getRuntime().addShutdownHook(Thread { app.stop() })

        val values = (0..1000).map { randomBytes(valueSize) }.toList()
        ReStorageClient("http://localhost:$port").createBucket(bucket)

        val watch = Stopwatch.createStarted()
        val counter = AtomicInteger()
        val rateLimiter = RateLimiter.create(rate.toDouble())
        val threads = (0..benchmarkThreads).map {
            thread(start = false) {
                val client = ReStorageClient("http://localhost:$port")
                while (true) {
                    rateLimiter.acquire()
                    val key = counter.getAndIncrement()
                    if (key > requests) break
                    if (key % reportInterval == 0 && watch.elapsed(TimeUnit.SECONDS) > 0) {
                        val speed = key / watch.elapsed(TimeUnit.SECONDS)
                        logger.info { "Speed: $speed rps; Requests performed: ${String.format("%,d",key)}" }
                    }
                    client.putObject(bucket, key.toString(), values.random())
                }
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }
        println("Total time: $watch")
        println("Request per second: ${requests / watch.elapsed(TimeUnit.SECONDS)}")
        app.stop()
    }
}

fun main(args: Array<String>) {
    Benchmark().main(args)
}


