package com.github.shahrivari.restorage.store

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import io.javalin.http.Context
import okhttp3.Response
import okhttp3.internal.http.HttpDate
import java.util.concurrent.ConcurrentHashMap

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MetaData(var bucket: String,
                    var key: String,
                    @JsonIgnore
                    var contentLength: Long? = null,
                    @JsonIgnore
                    var objectSize: Long? = null,
                    var contentType: String? = null,
                    @JsonIgnore
                    var lastModified: Long? = null,
                    var other: MutableMap<String, String> = ConcurrentHashMap()) {
    companion object {
        const val OBJECT_META_HEADER_PREFIX = "X-ReStorage-Object-Meta-"

        fun fromHttpHeaders(ctx: Context): MetaData {
            val metaData = MetaData(ctx.pathParam("bucket"),
                                    ctx.pathParam("key"),
                                    contentType = ctx.contentType())
            ctx.headerMap().filter { it.key.startsWith(OBJECT_META_HEADER_PREFIX) }.forEach {
                metaData.set(it.key.removePrefix(OBJECT_META_HEADER_PREFIX), it.value)
            }

            return metaData
        }

        fun fromResponse(bucket: String, key: String, response: Response): MetaData {
            val metaData = MetaData(bucket, key)
            metaData.contentLength = response.body()?.contentLength()
            metaData.contentType = response.body()?.contentType()?.toString()
            metaData.lastModified = response.header("Last-Modified")?.let { HttpDate.parse(it).time }

            response.headers().names().filter { it.startsWith(OBJECT_META_HEADER_PREFIX) }.forEach {
                metaData.set(it.removePrefix(OBJECT_META_HEADER_PREFIX), response.header(it) ?: "")
            }

            return metaData
        }
    }

    @Synchronized
    fun set(key: String, value: String) {
        other[key] = value
    }

    fun get(key: String) = other[key]

    fun fillToHeaders(ctx: Context) {
        ctx.contentType(contentType ?: "application/octet-stream")
        contentLength?.let { ctx.res.setContentLengthLong(it) }
        lastModified?.let {
            ctx.res.setDateHeader("Last-Modified", it)
            ctx.res.setIntHeader("Age", ((System.currentTimeMillis() - it) / 1000).toInt())
        }
        other.forEach {
            ctx.header("$OBJECT_META_HEADER_PREFIX${it.key}", it.value)
        }
    }

}