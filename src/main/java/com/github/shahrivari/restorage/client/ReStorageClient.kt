package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.KeyNotFoundException
import com.github.shahrivari.restorage.store.BucketInfo
import com.github.shahrivari.restorage.store.GetResult
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.PutResult
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okio.BufferedSink
import okio.Okio
import java.io.InputStream
import java.lang.IllegalArgumentException
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

    fun bucketNotExists(bucket: String) = !bucketExists(bucket)

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

    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult? {
        if (start == null && end != null) throw  IllegalArgumentException("start is null but end is not!")

        val builder = Request.Builder().url("$address/obj/$bucket/$key")
        if (start != null)
            builder.addHeader("Range", "$start-${end ?: ""}")
        val request: Request = builder.build()

        val response = pureClient.newCall(request).execute()
        val stream: InputStream = response.body()?.byteStream() ?: throw IllegalStateException("Body is null")
        val metaData = MetaData.fromResponse(bucket, key, response)
        return GetResult(bucket, key, stream, metaData)
    }

    fun getObjectMeta(bucket: String, key: String): MetaData? {
        restClient.getObjectMeta(bucket, key).execute().let {
            if (it.code() == 404)
                throw KeyNotFoundException(bucket, key)
            return it.body()
        }
    }

    fun objectExists(bucket: String, key: String): Boolean {
        restClient.getObjectMeta(bucket, key).execute().let {
            return it.code() == 200
        }
    }

    fun deleteObject(bucket: String, key: String) {
        restClient.deleteObject(bucket, key).execute().let {
            if (it.code() == 404)
                throw KeyNotFoundException(bucket, key)
        }
    }

}

