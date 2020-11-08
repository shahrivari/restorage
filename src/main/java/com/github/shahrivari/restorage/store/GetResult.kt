package com.github.shahrivari.restorage.store

import java.io.InputStream

data class GetResult(val bucket: String,
                     val key: String,
                     val stream: InputStream,
                     val metaData: MetaData? = null) {

    fun getAllBytes(): ByteArray =
            stream.use { it.readBytes() }
}