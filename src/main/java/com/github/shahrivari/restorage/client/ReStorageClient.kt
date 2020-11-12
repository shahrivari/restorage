package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.BucketAlreadyExists
import com.github.shahrivari.restorage.BucketNotFound
import com.github.shahrivari.restorage.KeyNotFoundException
import com.github.shahrivari.restorage.ReStorageException
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.store.BucketInfo
import com.github.shahrivari.restorage.store.GetResult
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.PutResult
import okhttp3.*
import okio.BufferedSink
import okio.Okio
import retrofit2.Response
import java.io.ByteArrayInputStream
import java.io.InputStream

class ReStorageClient(private val address: String) {
    private val pureClient = OkHttpClient.Builder().build()
    private val restClient = ReStorageRestClient.getClient(address, pureClient)

    private fun <T> checkErrors(response: Response<T>, bucket: String, key: String? = null) {
        if (!response.isSuccessful) {
            val json = response.errorBody()?.string() ?: return
            throw ReStorageException.fromJson(json, bucket, key)
        }
    }

    private fun checkErrors(response: okhttp3.Response, bucket: String, key: String? = null) {
        if (!response.isSuccessful) {
            val json = response.body()?.string() ?: return
            throw ReStorageException.fromJson(json, bucket, key)
        }
    }

    fun getBucket(bucket: String): BucketInfo? {
        restClient.getBucket(bucket).execute().let {
            checkErrors(it, bucket)
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
            checkErrors(it, bucket)
            return it.body()
        }
    }

    fun deleteBucket(bucket: String) {
        restClient.deleteBucket(bucket).execute().let {
            checkErrors(it, bucket)
        }
    }

    private fun mediaTypeOf(type: String?) =
            if (type == null) null else MediaType.parse(type)

    fun putObject(bucket: String,
                  key: String,
                  stream: InputStream,
                  contentType: String? = null,
                  metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, stream, contentType, metaData, false)

    fun putObject(bucket: String,
                  key: String,
                  value: ByteArray,
                  contentType: String? = null,
                  metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, ByteArrayInputStream(value), contentType, metaData, false)

    fun appendObject(bucket: String,
                     key: String,
                     stream: InputStream,
                     contentType: String? = null,
                     metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, stream, contentType, metaData, true)

    fun appendObject(bucket: String,
                     key: String,
                     value: ByteArray,
                     contentType: String? = null,
                     metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, ByteArrayInputStream(value), contentType, metaData, true)

    private fun doPut(bucket: String,
                      key: String,
                      stream: InputStream,
                      contentType: String? = null,
                      metaData: Map<String, String>? = null,
                      isAppend: Boolean): PutResult? {
        val body = buildBody(stream, contentType)
        val builder = getRequestBuilder(bucket, key, metaData)
        val request: Request = if (isAppend) builder.put(body).build() else builder.post(body).build()

        val response = pureClient.newCall(request).execute()
        checkErrors(response, bucket, key)
        val json = response.body()?.string() ?: return null
        return fromJson<PutResult>(json)
    }

    private fun getRequestBuilder(bucket: String, key: String,
                                  metaData: Map<String, String>?): Request.Builder {
        val builder = Request.Builder().url("$address/objects/$bucket/$key")
        val headerBuilder = Headers.Builder()
        metaData?.forEach {
            headerBuilder.add("${MetaData.OBJECT_META_HEADER_PREFIX}${it.key}", it.value)
        }
        builder.headers(headerBuilder.build())
        return builder
    }

    private fun buildBody(stream: InputStream, contentType: String?): RequestBody {
        return object : RequestBody() {

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
    }

    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult? {
        if (start == null && end != null) throw  IllegalArgumentException("start is null but end is not!")

        val builder = Request.Builder().url("$address/objects/$bucket/$key")
        if (start != null)
            builder.addHeader("Range", "bytes=$start-${end ?: ""}")
        val request: Request = builder.build()

        val response = pureClient.newCall(request).execute()
        checkErrors(response, bucket, key)
        val stream: InputStream = response.body()?.byteStream() ?: throw IllegalStateException("Body is null")
        val metaData = MetaData.fromResponse(bucket, key, response)
        return GetResult(bucket, key, stream, metaData)
    }

    fun getObjectMeta(bucket: String, key: String): MetaData? {
        restClient.getObjectMeta(bucket, key).execute().let {
            checkErrors(it, bucket, key)
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
            checkErrors(it, bucket, key)
        }
    }
}

