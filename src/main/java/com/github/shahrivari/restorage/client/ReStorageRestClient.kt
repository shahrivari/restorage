package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.commons.jacksonMapper
import com.github.shahrivari.restorage.store.DeleteResponse
import com.github.shahrivari.restorage.store.MetaData
import com.github.shahrivari.restorage.store.fs.BucketInfo
import okhttp3.OkHttpClient
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path

private const val BUCKET_CRUD_PATH = "buckets/{bucket}"
private const val OBJECT_CRUD_PATH = "objects/{bucket}/{key}"

interface ReStorageRestClient {
    @GET(BUCKET_CRUD_PATH)
    fun getBucket(@Path("bucket") bucket: String): Call<BucketInfo>

    @POST(BUCKET_CRUD_PATH)
    fun createBucket(@Path("bucket") bucket: String): Call<BucketInfo>

    @DELETE(BUCKET_CRUD_PATH)
    fun deleteBucket(@Path("bucket") bucket: String): Call<Unit>

    @DELETE(OBJECT_CRUD_PATH)
    fun deleteObject(@Path("bucket") bucket: String,
                     @Path("key") key: String): Call<DeleteResponse>

    @GET("$OBJECT_CRUD_PATH/meta")
    fun getObjectMeta(@Path("bucket") bucket: String,
                      @Path("key") key: String): Call<MetaData>

    @GET("$OBJECT_CRUD_PATH/md5")
    fun getObjectMd5(@Path("bucket") bucket: String,
                      @Path("key") key: String): Call<MetaData>

    companion object {

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
}
