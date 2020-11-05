package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.store.BucketInfo
import com.github.shahrivari.restorage.store.GetResult
import com.github.shahrivari.restorage.store.PutResult
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okio.BufferedSink
import okio.Okio
import java.io.InputStream
import java.lang.IllegalStateException

class ReStorageClient(private val address: String) {
    private val restClient = ReStorageRestClient.getClient(address)
    private val pureClient = OkHttpClient.Builder().build()


    fun getBucket(bucket: String): BucketInfo? {
        restClient.getBucket(bucket).execute().let {
            if (it.code() == 404)
                throw BucketNotFound(bucket)
            return it.body()
        }
    }

    fun bucketExists(bucket: String): Boolean {
        restClient.getBucket(bucket).execute().let {
            return it.isSuccessful
        }
    }

    fun createBucket(bucket: String): BucketInfo? {
        restClient.createBucket(bucket).execute().let {
            if (it.code() == 409)
                throw BucketAlreadyExists(bucket)
            return it.body()
        }
    }

    fun deleteBucket(bucket: String) {
        restClient.deleteBucket(bucket).execute().let {
            if (it.code() == 404)
                throw BucketNotFound(bucket)
        }
    }

    private fun mediaTypeOf(type: String?) =
            if (type == null) null else MediaType.parse(type)

    fun putObject(bucket: String, key: String, bytes: ByteArray, contentType: String? = null): PutResult? {
        val body = RequestBody.create(mediaTypeOf(contentType), bytes)
        return restClient.putObject(bucket, key, body).execute().body()
    }

    fun putObject(bucket: String, key: String, stream: InputStream, contentType: String? = null): PutResult? {
        val body = object : RequestBody() {

            override fun contentLength(): Long {
                return if (stream.available() > 0) stream.available().toLong() else -1
            }

            override fun contentType(): MediaType? =
                    mediaTypeOf(contentType)

            override fun writeTo(sink: BufferedSink) {
                Okio.source(stream).use {
                    sink.writeAll(it)
                }
            }
        }

        return restClient.putObject(bucket, key, body).execute().body()
    }

    fun getObject(bucket: String, key: String): GetResult? {
        val request: Request = Request.Builder()
                .url("$address/obj/$bucket/$key")
                .build()
        val response = pureClient.newCall(request).execute()
        val stream: InputStream = response.body()?.byteStream() ?: throw IllegalStateException("Body is null")
        val result = GetResult(bucket,
                               key,
                               stream,
                               response.body()?.contentLength(),
                               null,
                               null)
        return result
    }


}

