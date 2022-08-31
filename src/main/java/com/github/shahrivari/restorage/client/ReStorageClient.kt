package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.exception.ReStorageException
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.store.*
import com.github.shahrivari.restorage.store.fs.BucketInfo
import okhttp3.*
import okio.BufferedSink
import okio.Okio
import retrofit2.Response
import java.io.InputStream
import java.util.concurrent.TimeUnit

class ReStorageClient(private val address: String) {
    private val pureClient = OkHttpClient.Builder()
        .callTimeout(5, TimeUnit.MINUTES).readTimeout(5, TimeUnit.MINUTES).build()
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

    fun putObject(
        bucket: String,
        key: String,
        obj: ObjectToPut
    ): PutResult? =
        doPut(bucket, key, obj, false)


    fun putObject(
        bucket: String,
        key: String,
        bytes: ByteArray,
        metaData: Map<String, String>? = null
    ): PutResult? =
        doPut(bucket, key, ObjectToPut(bytes, metaData = metaData), false)

    fun appendObject(
        bucket: String,
        key: String,
        obj: ObjectToPut
    ): PutResult? =
        doPut(bucket, key, obj, true)

    fun appendObject(
        bucket: String,
        key: String,
        bytes: ByteArray,
        metaData: Map<String, String>? = null
    ): PutResult? =
        doPut(bucket, key, ObjectToPut(bytes, metaData = metaData), true)

    private fun doPut(
        bucket: String,
        key: String,
        obj: ObjectToPut,
        isAppend: Boolean
    ): PutResult? {
        val body = buildBody(obj.stream, obj.length, obj.contentType)
        val builder = getRequestBuilder(bucket, key, obj.metaData)

        val request: Request =
            if (isAppend) builder.put(body).build() else builder.post(body).build()

        val response = pureClient.newCall(request).execute()
        checkErrors(response, bucket, key)
        val json = response.body()?.string() ?: return null
        return fromJson<PutResult>(json)
    }

    private fun getRequestBuilder(
        bucket: String, key: String,
        metaData: Map<String, String>?
    ): Request.Builder {
        val builder = Request.Builder().url("$address/objects/$bucket/$key")
        val headerBuilder = Headers.Builder()
        metaData?.forEach {
            headerBuilder.add("${MetaData.OBJECT_META_HEADER_PREFIX}${it.key}", it.value)
        }
        builder.headers(headerBuilder.build())
        return builder
    }

    private fun buildBody(stream: InputStream, length: Long?, contentType: String?): RequestBody {
        return object : RequestBody() {

            override fun contentLength(): Long {
                if (length != null) return length
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

    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult {
        if (start == null && end != null) throw IllegalArgumentException("start is null but end is not!")

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

    fun getObjectMd5(bucket: String, key: String): MetaData? {
        restClient.getObjectMd5(bucket, key).execute().let {
            checkErrors(it, bucket, key)
            return it.body()
        }
    }

    fun objectExists(bucket: String, key: String): Boolean {
        restClient.getObjectMeta(bucket, key).execute().let {
            return it.code() == 200
        }
    }

    fun deleteObject(bucket: String, key: String): DeleteResponse? {
        restClient.deleteObject(bucket, key).execute().let {
            checkErrors(it, bucket, key)
            return it.body()
        }
    }

    fun generateHls(
        bucket: String,
        key: String,
    ): HlsCreationResult? {
        val request =
            Request.Builder().url("$address/hls/$bucket/$key").post(RequestBody.create(null, byteArrayOf())).build()
        val response = pureClient.newCall(request).execute()
        checkErrors(response, bucket, key)
        val json = response.body()?.string() ?: return null
        return fromJson<HlsCreationResult>(json)
    }

    fun getHlsFile(
        bucket: String,
        key: String,
        file:String
    ): ByteArray? {
        val request =
            Request.Builder().url("$address/hls/$bucket/$key/$file").get().build()
        val response = pureClient.newCall(request).execute()
        checkErrors(response, bucket, key)
        return response.body()?.bytes()
    }
}

data class ObjectToPut(
    val stream: InputStream,
    val length: Long? = null,
    val contentType: String? = null,
    val metaData: Map<String, String>? = null
) {
    constructor(
        data: ByteArray,
        length: Long? = null,
        contentType: String? = null,
        metaData: Map<String, String>? = null
    )
            : this(data.inputStream(), length, contentType, metaData)
}