package com.github.shahrivari.restorage.commons

import java.io.FileInputStream
import java.io.InputStream

class RangeStream(private val underlyingStream: InputStream, private var limit: Long) : InputStream() {

    companion object {
        fun getRangeStreamFromFile(path: String, offset: Long, size: Long = Long.MAX_VALUE): RangeStream {
            val stream = FileInputStream(path)
            stream.channel.position(offset)
            return RangeStream(stream, size)
        }
    }

    @Synchronized
    override fun read(): Int {
        if (limit <= 0)
            return -1
        limit--
        return underlyingStream.read()
    }
}