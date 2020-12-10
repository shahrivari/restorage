package com.github.shahrivari.restorage.store

data class DeleteResponse(val bucket: String,
                          val key: String,
                          val size: Long)