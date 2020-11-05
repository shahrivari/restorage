package com.github.shahrivari.restorage.commons

import java.io.BufferedInputStream
import java.io.InputStream

class RangeStream(private val underlyingStream: InputStream, private var limit: Long) : InputStream() {
    private val bufferedStream = BufferedInputStream(underlyingStream, 64 * 1024)

    override fun read(): Int {
        if (limit <= 0)
            return -1
        limit--
        return bufferedStream.read()
    }

    override fun close() {
        underlyingStream.close()
    }
}