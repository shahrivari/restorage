package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.store.MetaData.Companion.OBJECT_META_HEADER_PREFIX
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.entity.ContentType
import org.apache.http.entity.InputStreamEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager

class ReStorageRestClient(address: String = "http://localhost:7000") {

//    companion object{
//        private const val PORT = "7000"

//        fun getClient(address: String, client: OkHttpClient): ReStorageRestClient {
//            val retrofit = Retrofit.Builder()
//                    .baseUrl(address)
//                    .client(client)
//                    .addCallAdapterFactory(RxJava2CallAdapterFactory.create())
//                    .addConverterFactory(JacksonConverterFactory.create(jacksonMapper))
//                    .build()
//            return retrofit.create(ReStorageRestClient::class.java)
//        }
//    }
    private val ROOT_DIR = address
    private val BUCKET_CRUD_PATH = "$ROOT_DIR/buckets"
    private val OBJECT_CRUD_PATH = "$ROOT_DIR/objects"
//    private var httpClient = HttpClients.createDefault()
    private val cm = PoolingHttpClientConnectionManager()
    private val splitedAddress = address.split(":")
    private val host = HttpHost(splitedAddress[1].removePrefix("//"), splitedAddress[2].toInt())
    private val route = HttpRoute(host)
    init {
        cm.maxTotal=2048
        cm.defaultMaxPerRoute=2048
        cm.setMaxPerRoute(route,2048)
    }

    fun getClient(): CloseableHttpClient = HttpClients.custom()
            .setConnectionManager(cm)
            .build()

    fun getBucket(bucket: String): HttpResponse {
        val httpClient = getClient()
        val httpGet = HttpGet("$BUCKET_CRUD_PATH/$bucket")
        val httpresponse: HttpResponse = httpClient.execute(httpGet)
        return httpresponse
    }

    fun createBucket(bucket: String): HttpResponse {
        val httpClient = getClient()
        val httppost = HttpPost("$BUCKET_CRUD_PATH/$bucket")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpClient.execute(httppost)
        return httpresponse
    }

    fun deleteBucket(bucket: String): HttpResponse  {
        val httpClient = getClient()
        val httpdelete = HttpDelete("$BUCKET_CRUD_PATH/$bucket")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpClient.execute(httpdelete)
        return httpresponse
    }

    fun putObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {
        val httpClient = getClient()
        val httpPost = HttpPost("$OBJECT_CRUD_PATH/$bucket/$key")
//        val byteArrayOutputStream = ByteArrayOutputStream(data.size)
//        byteArrayOutputStream.write(data)
//        val entity = ByteArrayEntity(data)
        val entity = if (objectToPut.length != null && objectToPut.contentType != null)
            InputStreamEntity(objectToPut.stream, objectToPut.length, ContentType.parse(objectToPut.contentType))
        else InputStreamEntity(objectToPut.stream)
        objectToPut.metaData?.forEach { (key, value) ->
            httpPost.addHeader("$OBJECT_META_HEADER_PREFIX$key", value)
        }
//        val entity = InputStreamEntity(objectToPut.stream)
        httpPost.entity = entity
        val httpResponse: HttpResponse = httpClient.execute(httpPost)
        return httpResponse
    }


    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): HttpResponse {
        val httpClient = getClient()
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key")
        if (start != null)  httpGet.addHeader("Range", "bytes=$start-${end ?: ""}")
        val httpResponse = httpClient.execute(httpGet)
        return httpResponse
    }

    fun deleteObject(bucket: String, key: String): HttpResponse {
        val httpClient = getClient()
        val httpdelete = HttpDelete("$OBJECT_CRUD_PATH/$bucket/$key")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpClient.execute(httpdelete)
        return httpresponse
    }

    fun appendObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {
        val httpClient = getClient()
        val httpPut = HttpPut("$OBJECT_CRUD_PATH/$bucket/$key")
        val entity = if (objectToPut.length != null && objectToPut.contentType != null)
            InputStreamEntity(objectToPut.stream, objectToPut.length, ContentType.parse(objectToPut.contentType))
        else InputStreamEntity(objectToPut.stream)
        objectToPut.metaData?.forEach { (key, value) ->
            httpPut.addHeader(key, value)
        }
//        val entity = ByteArrayEntity(data)
        httpPut.entity = entity
        val httpResponse = httpClient.execute(httpPut)
        return httpResponse
    }

    fun getObjectMeta(bucket: String, key: String): HttpResponse {
        val httpClient = getClient()
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key/meta")
        val httpResponse = httpClient.execute(httpGet)
        return httpResponse
    }

    fun getObjectMd5(bucket: String, key: String): HttpResponse {
        val httpClient = getClient()
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key/md5")
        val httpResponse = httpClient.execute(httpGet)
        return httpResponse
    }

//    @GET(BUCKET_CRUD_PATH)
//    fun getBucket(@Path("bucket") bucket: String): Call<BucketInfo>
//
//    @POST(BUCKET_CRUD_PATH)
//    fun createBucket(@Path("bucket") bucket: String): Call<BucketInfo>
//
//    @DELETE(BUCKET_CRUD_PATH)
//    fun deleteBucket(@Path("bucket") bucket: String): Call<Unit>
//
//    @DELETE(OBJECT_CRUD_PATH)
//    fun deleteObject(@Path("bucket") bucket: String,
//                     @Path("key") key: String): Call<DeleteResponse>
//
//    @GET("$OBJECT_CRUD_PATH/meta")
//    fun getObjectMeta(@Path("bucket") bucket: String,
//                      @Path("key") key: String): Call<MetaData>
//
//    @GET("$OBJECT_CRUD_PATH/md5")
//    fun getObjectMd5(@Path("bucket") bucket: String,
//                      @Path("key") key: String): Call<MetaData>

}
