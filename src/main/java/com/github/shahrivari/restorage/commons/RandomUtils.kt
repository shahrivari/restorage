package com.github.shahrivari.restorage.commons

import java.util.*

fun randomBytes(size: Int): ByteArray {
    val bytes = ByteArray(size)
    Random().nextBytes(bytes)
    return bytes
}