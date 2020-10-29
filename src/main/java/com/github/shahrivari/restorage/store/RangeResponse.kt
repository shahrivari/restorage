package com.github.shahrivari.restorage.store

import java.io.InputStream

class RangeResponse(val start: Long = 0, val end: Long = 0, val stream: InputStream) {
    val size
        get() = start - end
}


