package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.RangeHeader
import com.github.shahrivari.restorage.exception.BucketNotFound
import com.github.shahrivari.restorage.exception.InvalidRangeRequest
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.fs.FileSystemStore
import io.javalin.http.Context

class Controller(private val store: FileSystemStore) {

    companion object {
        const val BUCKET_CRUD_PATH = "/buckets/:bucket"
        const val OBJECT_CRUD_PATH = "/objects/:bucket/:key"
    }

    fun Context.bucket() = pathParam("bucket")
    fun Context.key() = pathParam("key")

    fun deleteObject(ctx: Context) {
        store.delete(ctx.bucket(), ctx.key())
        ctx.status(200)
    }

    fun headObject(ctx: Context) {
        val result = store.getMeta(ctx.bucket(), ctx.key())
        result.fillToHeaders(ctx)
        result.objectSize?.let { ctx.res.setContentLengthLong(it) }
    }

    fun getObjectMeta(ctx: Context) {
        val result = store.getMeta(ctx.bucket(), ctx.key())
        ctx.status(200)
        ctx.json(result)
    }

    fun getObject(ctx: Context) {
        var start: Long? = null
        var end: Long? = null

        val rangeHeader = ctx.header("Range")
        if (rangeHeader != null) {
            val ranges = RangeHeader.decodeRange(rangeHeader)
            if (ranges.size != 1)
                throw InvalidRangeRequest(
                        rangeHeader)
            start = ranges.first().start
            end = ranges.first().end
        }

        val result = store.get(ctx.bucket(), ctx.key(), start, end)
        ctx.header("Content-Encoding", "identity")
        result.metaData?.fillToHeaders(ctx)

        if (start == null && end == null)
            ctx.status(200)
        else
            ctx.status(206)

        ctx.result(result.stream)
    }

    fun putChunk(ctx: Context) {
        val result = store.append(ctx.bucket(),
                                  ctx.key(),
                                  ctx.req.inputStream,
                                  MetaData.fromHttpHeaders(ctx))
        ctx.json(result)
    }

    fun putObject(ctx: Context) {
        val result = store.put(ctx.bucket(),
                               ctx.key(),
                               ctx.req.inputStream,
                               MetaData.fromHttpHeaders(ctx))
        ctx.json(result)
    }

    fun deleteBucket(ctx: Context) {
        store.deleteBucket(ctx.bucket())
        ctx.status(204)
    }

    fun headBucket(ctx: Context) {
        val result = store.getBucketInfo(ctx.bucket())
        ctx.status(if (result.isPresent) 200 else 404)
    }

    fun getBucketInfo(ctx: Context) {
        val result = store.getBucketInfo(ctx.bucket())
        if (!result.isPresent)
            throw BucketNotFound(ctx.bucket())
        result.ifPresent { ctx.json(it) }
    }

    fun createBucket(ctx: Context) {
        val result = store.createBucket(ctx.bucket())
        ctx.json(result)
    }
}