package com.github.shahrivari.restorage.store

import com.fasterxml.jackson.annotation.JsonInclude
import io.javalin.http.Context
import okhttp3.internal.http.HttpDate
import org.apache.http.HttpResponse
import java.util.concurrent.ConcurrentHashMap

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MetaData(val bucket: String,
                    val key: String,
                    var contentLength: Long? = null,
                    var objectSize: Long? = null,
                    var lastModified: Long? = null,
                    var contentType: String? = null,
                    val other: MutableMap<String, String> = ConcurrentHashMap()) {
    companion object {
        const val OBJECT_META_HEADER_PREFIX = "X-ReStorage-Object-Meta-"
        const val OBJECT_SIZE_HEADER = "X-ReStorage-Object-Size"

        fun fromHttpHeaders(ctx: Context): MetaData {
            val metaData = MetaData(ctx.pathParam("bucket"),
                                    ctx.pathParam("key"),
                                    contentType = ctx.contentType())
            ctx.headerMap().filter { it.key.startsWith(OBJECT_META_HEADER_PREFIX) }.forEach {
                metaData.set(it.key.removePrefix(OBJECT_META_HEADER_PREFIX), it.value)
            }

            return metaData
        }

        fun fromResponse(bucket: String, key: String, response: HttpResponse): MetaData {
            val metaData = MetaData(bucket, key)
            metaData.contentLength = response.entity.contentLength
            metaData.contentType = response.entity.contentType.value
            metaData.objectSize = response.getFirstHeader(OBJECT_SIZE_HEADER).value.toLongOrNull()
            metaData.lastModified = response.getFirstHeader("Last-Modified")?.let {
                HttpDate.parse(it.value).time
            }
            response.allHeaders.filter { it.name.startsWith(OBJECT_META_HEADER_PREFIX) }.forEach {
                metaData.set(it.name.removePrefix(OBJECT_META_HEADER_PREFIX), it.value ?: "")
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
        objectSize?.let { ctx.res.setHeader(OBJECT_SIZE_HEADER, it.toString()) }
        lastModified?.let {
            ctx.res.setDateHeader("Last-Modified", it)
            ctx.res.setIntHeader("Age", ((System.currentTimeMillis() - it) / 1000).toInt())
        }
        other.forEach {
            ctx.header("$OBJECT_META_HEADER_PREFIX${it.key}", it.value)
        }
    }

}