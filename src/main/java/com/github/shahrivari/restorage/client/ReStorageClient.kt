package com.github.shahrivari.restorage.client

//import okhttp3.*
//import okio.BufferedSink
//import okio.Okio
//import org.apache.http.HttpResponse
//import retrofit2.Response
import com.github.shahrivari.restorage.commons.fromJson
import com.github.shahrivari.restorage.exception.ReStorageException
import com.github.shahrivari.restorage.store.DeleteResponse
import com.github.shahrivari.restorage.store.GetResult
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.PutResult
import com.github.shahrivari.restorage.store.fs.BucketInfo
import org.apache.http.HttpResponse
import java.io.InputStream

class ReStorageClient(private val address: String) {
//    private val pureClient = OkHttpClient.Builder().build()
    private val restClient = ReStorageRestClient()

//    private fun <T> checkErrors(response: Response<T>, bucket: String, key: String? = null) {
//        if (!response.isSuccessful) {
//            val json = response.errorBody()?.string() ?: return
//            throw ReStorageException.fromJson(json, bucket, key)
//        }
//    }

//    private fun checkErrors(response: okhttp3.Response, bucket: String, key: String? = null) {
//        if (!response.isSuccessful) {
//            val json = response.body()?.string() ?: return
//            throw ReStorageException.fromJson(json, bucket, key)
//        }
//    }

    private fun checkErrors(response: HttpResponse, bucket: String, key: String? = null) {
        if (response.statusLine.statusCode!=200 && response.statusLine.statusCode!=204 ) {
            val json = String(response.entity.content.readBytes())
            throw ReStorageException.fromJson(json, bucket, key)
        }
    }

    fun getBucket(bucket: String): BucketInfo? {
//        return restClient.getBucket(bucket)
        restClient.getBucket(bucket).let {
            checkErrors(it, bucket)
            return fromJson<BucketInfo>(String(it.entity.content.readBytes()))
        }
    }

    fun bucketExists(bucket: String): Boolean {
        return restClient.getBucket(bucket).statusLine.statusCode==200
//        restClient.getBucket(bucket).execute().let {
//            return it.isSuccessful
//        }
    }

    fun bucketNotExists(bucket: String) = !bucketExists(bucket)

    fun createBucket(bucket: String): BucketInfo? {
        restClient.createBucket(bucket).let {
            checkErrors(it, bucket)
            return fromJson(String(it.entity.content.readBytes()))
        }
    }

    fun deleteBucket(bucket: String) {
        checkErrors(restClient.deleteBucket(bucket), bucket)
    }

//    private fun mediaTypeOf(type: String?) =
//            if (type == null) null else MediaType.parse(type)

    fun putObject(bucket: String,
                  key: String,
                  obj: ObjectToPut): PutResult? =
            doPut(bucket, key, obj, false)


    fun putObject(bucket: String,
                  key: String,
                  bytes: ByteArray,
                  metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, ObjectToPut(bytes, metaData = metaData), false)

    fun appendObject(bucket: String,
                     key: String,
                     obj: ObjectToPut): PutResult? =
            doPut(bucket, key, obj, true)

    fun appendObject(bucket: String,
                     key: String,
                     bytes: ByteArray,
                     metaData: Map<String, String>? = null): PutResult? =
            doPut(bucket, key, ObjectToPut(bytes, metaData = metaData), true)

    private fun doPut(bucket: String,
                      key: String,
                      obj: ObjectToPut,
                      isAppend: Boolean): PutResult? {
        val response = if (isAppend) restClient.appendObject(bucket, key, obj) else restClient.putObject(bucket, key, obj)
        checkErrors(response, bucket, key)
        return fromJson<PutResult>(String(response.entity.content.readBytes()))
    }
//    private fun doPut(bucket: String,
//                      key: String,
//                      obj: ObjectToPut,
//                      isAppend: Boolean): PutResult? {
//        val body = buildBody(obj.stream, obj.length, obj.contentType)
//        val builder = getRequestBuilder(bucket, key, obj.metaData)
//
//        val request: Request =
//                if (isAppend) builder.put(body).build() else builder.post(body).build()
//
//        val response = pureClient.newCall(request).execute()
//        checkErrors(response, bucket, key)
//        val json = response.body()?.string() ?: return null
//        return fromJson<PutResult>(json)
//    }

//    private fun getRequestBuilder(bucket: String, key: String,
//                                  metaData: Map<String, String>?): Request.Builder {
//        val builder = Request.Builder().url("$address/objects/$bucket/$key")
//        val headerBuilder = Headers.Builder()
//        metaData?.forEach {
//            headerBuilder.add("${MetaData.OBJECT_META_HEADER_PREFIX}${it.key}", it.value)
//        }
//        builder.headers(headerBuilder.build())
//        return builder
//    }
//
//    private fun buildBody(stream: InputStream, length: Long?, contentType: String?): RequestBody {
//        return object : RequestBody() {
//
//            override fun contentLength(): Long {
//                if (length != null) return length
//                return if (stream.available() > 0) stream.available().toLong() else -1
//            }
//
//            override fun contentType(): MediaType? =
//                    mediaTypeOf(contentType)
//
//            override fun writeTo(sink: BufferedSink) {
//                Okio.source(stream).use {
//                    sink.writeAll(it)
//                }
//            }
//        }
//    }

    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): GetResult? {
        if (start == null && end != null) throw  IllegalArgumentException(
                "start is null but end is not!")

//        val builder = Request.Builder().url("$address/objects/$bucket/$key")
//        if (start != null)
//            builder.addHeader("Range", "bytes=$start-${end ?: ""}")
//        val request: Request = builder.build()

//        val response = pureClient.newCall(request).execute()
        val response =  restClient.getObject(bucket, key, start, end)
        checkErrors(response, bucket, key)
        val stream: InputStream = response.entity.content ?: throw IllegalStateException(
                "Body is null")
        val metaData = MetaData.fromResponse(bucket, key, response)
        return GetResult(bucket, key, stream, metaData)
    }

    fun getObjectMeta(bucket: String, key: String): MetaData? {
        restClient.getObjectMeta(bucket, key).let {
            checkErrors(it, bucket, key)
            return fromJson(String(it.entity.content.readBytes()))
        }
    }

    fun getObjectMd5(bucket: String, key: String): MetaData? {
        restClient.getObjectMd5(bucket, key).let {
            checkErrors(it, bucket, key)
            return fromJson(String(it.entity.content.readBytes()))
        }
    }

    fun objectExists(bucket: String, key: String): Boolean {
        restClient.getObjectMeta(bucket, key).let {
            return it.statusLine.statusCode == 200
        }
    }

    fun deleteObject(bucket: String, key: String): DeleteResponse? {
        restClient.deleteObject(bucket, key).let {
            checkErrors(it, bucket, key)
            return fromJson(String(it.entity.content.readBytes()))
        }
    }
}

data class ObjectToPut(
        val stream: InputStream,
        val length: Long? = null,
        val contentType: String? = null,
        val metaData: Map<String, String>? = null
) {
    constructor(data: ByteArray,
                length: Long? = null,
                contentType: String? = null,
                metaData: Map<String, String>? = null)
            : this(data.inputStream(), length, contentType, metaData)
}