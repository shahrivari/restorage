package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.RangeHeader
import com.github.shahrivari.restorage.exception.BucketNotFoundException
import com.github.shahrivari.restorage.exception.InvalidRangeRequestException
import com.github.shahrivari.restorage.store.DeleteResponse
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.fs.FileSystemStore
import io.javalin.http.Context
import mu.KotlinLogging

class Controller(private val store: FileSystemStore) {
    private val logger = KotlinLogging.logger {}

    companion object {
        const val BUCKET_CRUD_PATH = "/buckets/:bucket"
        const val OBJECT_CRUD_PATH = "/objects/:bucket/:key"
    }

    fun Context.bucket() = pathParam("bucket")
    fun Context.key() = pathParam("key")

    fun deleteObject(ctx: Context) {
        val size = store.delete(ctx.bucket(), ctx.key())
        ctx.json(DeleteResponse(ctx.bucket(), ctx.key(), size))
    }

    fun headObject(ctx: Context) {
        val result = store.getMeta(ctx.bucket(), ctx.key())
        ctx.header("Content-Encoding", "identity")
        ctx.header("Accept-Ranges", "bytes")
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
                throw InvalidRangeRequestException(rangeHeader)
            start = ranges.first().start
            end = ranges.first().end
        }

        logger.debug { "Range: $start-$end" }

        val result = store.get(ctx.bucket(), ctx.key(), start, end)
        ctx.header("Content-Encoding", "identity")
        ctx.header("Accept-Ranges", "bytes")
        if (rangeHeader != null) {
            val endStr = end?.toString() ?: ""
            ctx.header("Content-Range", "bytes $start-$endStr/${result.metaData?.objectSize}")
        }

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
            throw BucketNotFoundException(ctx.bucket())
        result.ifPresent { ctx.json(it) }
    }

    fun createBucket(ctx: Context) {
        val result = store.createBucket(ctx.bucket())
        ctx.json(result)
    }

    fun getMd5(ctx: Context) {
        val meta = store.getMeta(ctx.bucket(), ctx.key())
        val md5 = store.computeMd5(ctx.bucket(), ctx.key())
        meta.set("md5", md5)
        ctx.status(200)
        ctx.json(meta)
    }
}