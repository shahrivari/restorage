package com.github.shahrivari.restorage.store

import java.io.InputStream

data class GetResult(val bucket: String,
                     val key: String,
                     val stream: InputStream,
                     val length: Long? = null,
                     val totalSize: Long? = null,
                     val lastModified: Long? = null,
                     val contentType: String? = null)