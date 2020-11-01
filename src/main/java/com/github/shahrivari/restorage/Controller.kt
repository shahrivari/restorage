package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.RangeHeader
import com.github.shahrivari.restorage.store.FileSystemBasedStore
import com.github.shahrivari.restorage.store.MetaData
import io.javalin.Javalin
import io.javalin.http.Context

class Controller(private val app: Javalin, private val store: FileSystemBasedStore) {

    fun Context.bucket() = pathParam("bucket")
    fun Context.key() = pathParam("key")

    fun wireUp() {
        app.post("/bucket/create/:bucket") { ctx ->
            store.createBucket(ctx.bucket())
            ctx.status(200)
        }

        app.get("/bucket/exists/:bucket") { ctx ->
            val result = store.bucketExists(ctx.bucket())
            ctx.status(if (result) 200 else 404)
            ctx.json(result)
        }

        app.delete("/bucket/delete/:bucket") { ctx ->
            store.deleteBucket(ctx.bucket())
            ctx.status(200)
        }

        app.post("/put/:bucket/:key") { ctx ->
            store.put(ctx.bucket(),
                      ctx.key(),
                      ctx.req.inputStream,
                      MetaData.fromHttpHeaders(ctx))
            ctx.status(200)
        }

        app.put("/append/:bucket/:key") { ctx ->
            store.append(ctx.bucket(),
                         ctx.key(),
                         ctx.req.inputStream)
            ctx.status(200)
        }

        app.get("/get/:bucket/:key") { ctx ->
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
            result.contentType?.let { ctx.contentType(it) }
            result.length?.let { ctx.res.setContentLengthLong(it) }
            result.lastModified?.let {
                ctx.res.setDateHeader("Last-Modified", it)
                ctx.res.setIntHeader("Age", ((System.currentTimeMillis() - it) / 1000).toInt())
            }

            if (start == null && end == null)
                ctx.status(200)
            else
                ctx.status(206)
        }

        app.get("/exists/:bucket/:key") { ctx ->
            val result = store.objectExists(ctx.bucket(), ctx.key())
            ctx.status(if (result) 200 else 404)
            ctx.json(result)
        }

        app.delete("/delete/:bucket/:key") { ctx ->
            store.delete(ctx.bucket(), ctx.key())
            ctx.status(200)
        }


        app.exception(StoraValidationException::class.java) { e, ctx ->
            ctx.status(e.statusCode)
            ctx.json(mapOf("code" to e.errorCode, "message" to e.message))
        }

        app.after { ctx ->
            ctx.res.setHeader("Server", "ReStorage")
        }

    }


}