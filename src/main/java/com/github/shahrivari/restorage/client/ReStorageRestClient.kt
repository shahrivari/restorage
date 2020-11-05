package com.github.shahrivari.restorage.client

import com.github.shahrivari.restorage.commons.jacksonMapper
import com.github.shahrivari.restorage.store.BucketInfo
import com.github.shahrivari.restorage.store.PutResult
import okhttp3.RequestBody
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.http.*

private const val BUCKET_CRUD_PATH = "buckets/{bucket}"
private const val OBJECT_CRUD_PATH = "obj/{bucket}/{key}"

interface ReStorageRestClient {
    @GET(BUCKET_CRUD_PATH)
    fun getBucket(@Path("bucket") bucket: String): Call<BucketInfo>

    @POST(BUCKET_CRUD_PATH)
    fun createBucket(@Path("bucket") bucket: String): Call<BucketInfo>

    @DELETE(BUCKET_CRUD_PATH)
    fun deleteBucket(@Path("bucket") bucket: String): Call<Unit>

    @POST(OBJECT_CRUD_PATH)
    fun putObject(@Path("bucket") bucket: String,
                  @Path("key") key: String,
                  @Body bytes: RequestBody): Call<PutResult>

    @HEAD(OBJECT_CRUD_PATH)
    fun getObjectMeta(@Path("bucket") bucket: String,
                      @Path("key") key: String): Call<Map<String, Any?>>


    companion object {

        fun getClient(address: String): ReStorageRestClient {
            val retrofit = Retrofit.Builder()
                    .baseUrl(address)
                    .addCallAdapterFactory(RxJava2CallAdapterFactory.create())
                    .addConverterFactory(JacksonConverterFactory.create(jacksonMapper))
                    .build()
            return retrofit.create(ReStorageRestClient::class.java)
        }
    }
}
