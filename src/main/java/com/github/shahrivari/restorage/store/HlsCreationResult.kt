package com.github.shahrivari.restorage.store

data class HlsCreationResult(
    val bucket: String,
    val key: String,
    val size: Long,
    val width: Int?,
    val height: Int?,
    val bitRate:Int?,
    val videoSize:Long?,
    val audioSize:Long?,
    val url:String? = null
)