package com.github.shahrivari.restorage.store

import com.fasterxml.jackson.annotation.JsonInclude
import io.javalin.http.Context

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MetaData(val bucket: String,
                    val key: String,
                    val contentType: String? = null,
                    val creationTime: Long = System.currentTimeMillis()) {
    companion object {

        fun fromHttpHeaders(ctx: Context): MetaData {
            return MetaData(ctx.pathParam("bucket"),
                            ctx.pathParam("key"),
                            contentType = ctx.contentType())
        }
    }

}