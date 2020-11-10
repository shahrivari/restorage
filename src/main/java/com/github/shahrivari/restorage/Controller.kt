package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.RangeHeader
import com.github.shahrivari.restorage.store.FileSystemBasedStore
import com.github.shahrivari.restorage.store.MetaData
import io.javalin.Javalin
import io.javalin.http.Context

class Controller(private val app: Javalin, private val store: FileSystemBasedStore) {

    companion object {
        const val BUCKET_CRUD_PATH = "/buckets/:bucket"
        const val OBJECT_CRUD_PATH = "/obj/:bucket/:key"
    }

    fun Context.bucket() = pathParam("bucket")
    fun Context.key() = pathParam("key")

    fun wireUp() {

        app.post(BUCKET_CRUD_PATH) { ctx ->
            val result = store.createBucket(ctx.bucket())
            ctx.status(200)
            ctx.json(result)
        }

        app.get(BUCKET_CRUD_PATH) { ctx ->
            val result = store.getBucketInfo(ctx.bucket())
            ctx.status(if (result.isPresent) 200 else 404)
            result.ifPresent { ctx.json(it) }
        }

        app.head(BUCKET_CRUD_PATH) { ctx ->
            val result = store.getBucketInfo(ctx.bucket())
            ctx.status(if (result.isPresent) 200 else 404)
        }

        app.delete(BUCKET_CRUD_PATH) { ctx ->
            store.deleteBucket(ctx.bucket())
            ctx.status(204)
        }

        app.post(OBJECT_CRUD_PATH) { ctx ->
            val result = store.put(ctx.bucket(),
                                   ctx.key(),
                                   ctx.req.inputStream,
                                   MetaData.fromHttpHeaders(ctx))
            ctx.json(result)
        }

        app.put(OBJECT_CRUD_PATH) { ctx ->
            val result = store.append(ctx.bucket(),
                         ctx.key(),
                         ctx.req.inputStream,
                         MetaData.fromHttpHeaders(ctx))
            ctx.json(result)
        }

        app.get(OBJECT_CRUD_PATH) { ctx ->
            var start: Long? = null
            var end: Long? = null

            val rangeHeader = ctx.header("Range")
            if (rangeHeader != null) {
                val ranges = RangeHeader.decodeRange(rangeHeader)
                if (ranges.size != 1)
                    throw InvalidRangeRequest(rangeHeader)
                start = ranges.first().start
                end = ranges.first().end
            }

            val result = store.get(ctx.bucket(), ctx.key(), start, end)
            ctx.result(result.stream)
            ctx.header("Content-Encoding", "identity")
            result.metaData?.fillToHeaders(ctx)

            if (start == null && end == null)
                ctx.status(200)
            else
                ctx.status(206)
        }

        app.get("$OBJECT_CRUD_PATH/meta") { ctx ->
            val result = store.getMeta(ctx.bucket(), ctx.key())
            ctx.status(200)
            ctx.json(result)
        }

        app.head(OBJECT_CRUD_PATH) { ctx ->
            val result = store.getMeta(ctx.bucket(), ctx.key())
            result.fillToHeaders(ctx)
            result.objectSize?.let { ctx.res.setContentLengthLong(it) }
        }

        app.delete(OBJECT_CRUD_PATH) { ctx ->
            store.delete(ctx.bucket(), ctx.key())
            ctx.status(200)
        }


        app.exception(ReStorageException::class.java) { e, ctx ->
            ctx.status(e.statusCode)
            ctx.json(mapOf("errorCode" to e.errorCode, "message" to e.message))
        }

        app.after { ctx ->
            ctx.res.setHeader("Server", "ReStorage")
        }

    }


}