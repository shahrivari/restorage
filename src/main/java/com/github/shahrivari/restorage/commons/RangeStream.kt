package com.github.shahrivari.restorage.commons

import java.io.InputStream

class RangeStream(private val underlyingStream: InputStream, private var limit: Long) : InputStream() {
    @Synchronized
    override fun read(): Int {
        if (limit <= 0)
            return -1
        limit--
        return underlyingStream.read()
    }
}