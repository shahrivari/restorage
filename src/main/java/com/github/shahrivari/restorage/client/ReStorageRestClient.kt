package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.commons.jacksonMapper
import okhttp3.OkHttpClient
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ContentType
import org.apache.http.entity.InputStreamEntity
import org.apache.http.impl.client.HttpClients
import retrofit2.Retrofit
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory
import retrofit2.converter.jackson.JacksonConverterFactory

interface ReStorageRestClient {

    companion object{
        private const val PORT = "7000"
        private const val ROOT_DIR = "http://localhost:$PORT"
        private const val BUCKET_CRUD_PATH = "$ROOT_DIR/buckets"
        private const val OBJECT_CRUD_PATH = "$ROOT_DIR/objects"
        fun getClient(address: String, client: OkHttpClient): ReStorageRestClient {
            val retrofit = Retrofit.Builder()
                    .baseUrl(address)
                    .client(client)
                    .addCallAdapterFactory(RxJava2CallAdapterFactory.create())
                    .addConverterFactory(JacksonConverterFactory.create(jacksonMapper))
                    .build()
            return retrofit.create(ReStorageRestClient::class.java)
        }
    }
    fun getBucket(bucket: String): HttpResponse {
        val httpclient = HttpClients.createDefault()
        val httpGet = HttpGet("$BUCKET_CRUD_PATH/$bucket")
        val httpresponse: HttpResponse = httpclient.execute(httpGet)
        return httpresponse
    }

    fun createBucket(bucket: String): HttpResponse {
        val httpclient = HttpClients.createDefault()
        val httppost = HttpPost("$BUCKET_CRUD_PATH/$bucket")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpclient.execute(httppost)
        return httpresponse
    }

    fun deleteBucket(bucket: String): HttpResponse  {
        val httpclient = HttpClients.createDefault()
        val httpdelete = HttpDelete("$BUCKET_CRUD_PATH/$bucket")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpclient.execute(httpdelete)
        return httpresponse
    }

    fun putObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {
        val httpclient = HttpClients.createDefault()
        val httpPost = HttpPost("$OBJECT_CRUD_PATH/$bucket/$key")
//        val byteArrayOutputStream = ByteArrayOutputStream(data.size)
//        byteArrayOutputStream.write(data)
//        val entity = ByteArrayEntity(data)
        val entity = if (objectToPut.length != null && objectToPut.contentType != null)
            InputStreamEntity(objectToPut.stream, objectToPut.length, ContentType.parse(objectToPut.contentType))
        else InputStreamEntity(objectToPut.stream)
        objectToPut.metaData?.forEach { (key, value) ->
            httpPost.addHeader(key, value)
        }
//        val entity = InputStreamEntity(objectToPut.stream)
        httpPost.entity = entity
        val httpResponse: HttpResponse = httpclient.execute(httpPost)
        return httpResponse
    }


    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): HttpResponse {
        val httpClient = HttpClients.createDefault()
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key")
        if (start != null)  httpGet.addHeader("Range", "bytes=$start-${end ?: ""}")
        val httpResponse = httpClient.execute(httpGet)
        return httpResponse
    }

    fun deleteObject(bucket: String, key: String): HttpResponse {
        val httpclient = HttpClients.createDefault()
        val httpdelete = HttpDelete("$OBJECT_CRUD_PATH/$bucket/$key")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
        val httpresponse: HttpResponse = httpclient.execute(httpdelete)
        return httpresponse
    }

    fun appendObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {
        val httpClient = HttpClients.createDefault()
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
        val httpClient = HttpClients.createDefault()
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key/meta")
        val httpResponse = httpClient.execute(httpGet)
        return httpResponse
    }

    fun getObjectMd5(bucket: String, key: String): HttpResponse {
        val httpClient = HttpClients.createDefault()
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
