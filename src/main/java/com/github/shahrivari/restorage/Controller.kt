package com.github.shahrivari.restorage

import com.github.shahrivari.restorage.commons.RangeHeader
import com.github.shahrivari.restorage.store.FileSystemBasedStore
import io.javalin.Javalin
import java.io.ByteArrayInputStream

class Controller(private val app: Javalin, private val store: FileSystemBasedStore) {

    fun wirePaths() {
        app.post("/bucket/create/:bucket") { ctx ->
            store.createBucket(ctx.pathParam("bucket"))
            ctx.status(200)
        }

        app.get("/bucket/exists/:bucket") { ctx ->
            val result = store.bucketExists(ctx.pathParam("bucket"))
            ctx.status(if (result) 200 else 404)
            ctx.json(result)
        }

        app.delete("/bucket/delete/:bucket") { ctx ->
            store.deleteBucket(ctx.pathParam("bucket"))
            ctx.status(200)
        }

        app.post("/put/:bucket/:key") { ctx ->
            store.put(ctx.pathParam("bucket"),
                      ctx.pathParam("key"),
                      ByteArrayInputStream(ctx.bodyAsBytes()),
                      ctx.contentType())
            ctx.status(200)
        }

        app.put("/append/:bucket/:key") { ctx ->
            store.append(ctx.pathParam("bucket"),
                         ctx.pathParam("key"),
                         ByteArrayInputStream(ctx.bodyAsBytes()))
            ctx.status(200)
        }

        app.get("/get/:bucket/:key") { ctx ->
            val rangeHeader = ctx.header("Range")
            if (rangeHeader == null) {
                val result = store.get(ctx.pathParam("bucket"), ctx.pathParam("key"))
                ctx.result(result)
                ctx.status(200)
            } else {
                //TODO: range request is very bad implemented!!!
                var ranges = RangeHeader.decodeRange(rangeHeader)
                if (ranges.size > 1)
                    throw InvalidRangeRequest(rangeHeader)
                val result = store.getRange(ctx.pathParam("bucket"),
                                            ctx.pathParam("key"),
                                            ranges.first().start,
                                            ranges.first().length())
                ctx.result(result.stream)
                ctx.status(200)
            }
        }
    }

    fun wireExceptions() {
        app.exception(StoraValidationException::class.java) { e, ctx ->
            ctx.status(e.statusCode)
            ctx.json(mapOf("code" to e.errorCode, "message" to e.message))
        }
    }

}