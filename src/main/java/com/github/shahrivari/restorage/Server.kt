package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.store.FileSystemBasedStore
import io.javalin.Javalin

fun main() {
    val app = Javalin.create()
    val store = FileSystemBasedStore("/dev/shm/store")
    val controller = Controller(app, store)
    controller.wirePaths()
    controller.wireExceptions()
    app.start(7000)
}