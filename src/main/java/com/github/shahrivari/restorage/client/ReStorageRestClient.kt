package ir.sls.stora.restorageClient

import com.github.shahrivari.restorage.store.MetaData.Companion.OBJECT_META_HEADER_PREFIX
import org.apache.http.HttpHeaders
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
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class ReStorageRestClient(address: String = "http://localhost:9090") {

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
    private val builder  : HttpClientBuilder
    //    private val httpClient: CloseableHttpClient
    private val executers = Executors.newFixedThreadPool(512)
    private val httpClient : CloseableHttpClient
    init {
        cm.maxTotal=128
        cm.defaultMaxPerRoute=128
        cm.setMaxPerRoute(route,128)
//        httpClient = HttpClients.custom()
//                .setConnectionManager(cm)
//                .build()
        builder = HttpClients.custom().setConnectionManager(cm)
        httpClient = getClient()
    }
    private fun getClient(): CloseableHttpClient = builder.build()

    fun getBucket(bucket: String): HttpResponse {
        val callableTask = Callable {
//            val httpClient = getClient()
            val httpGet = HttpGet("$BUCKET_CRUD_PATH/$bucket")
            val httpresponse: HttpResponse = httpClient.execute(httpGet)
            return@Callable httpresponse
        }
        return executers.submit(callableTask).get()
    }

    fun createBucket(bucket: String): HttpResponse {
        val callableTask  = Callable {
            val httppost = HttpPost("$BUCKET_CRUD_PATH/$bucket")
            return@Callable httpClient.execute(httppost)
        }
        return executers.submit(callableTask).get()
    }

    fun deleteBucket(bucket: String): HttpResponse  {
        val callableTask  = Callable {
            val httpdelete = HttpDelete("$BUCKET_CRUD_PATH/$bucket")
            val httpresponse: HttpResponse = httpClient.execute(httpdelete)
            return@Callable httpresponse
        }
        return executers.submit(callableTask).get()
    }

    fun putObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {

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
//        Connection: keep-alive
        httpPost.addHeader("Connection", "keep-alive")
        val callableTask  = Callable {
            val httpResponse: HttpResponse = httpClient.execute(httpPost)
            return@Callable httpResponse
        }
        return executers.submit(callableTask).get()
    }


    fun getObject(bucket: String, key: String, start: Long? = null, end: Long? = null): HttpResponse {
        val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key")
        if (start != null) httpGet.addHeader("Range", "bytes=$start-${end ?: ""}")
        httpGet.addHeader("Connection", "keep-alive")

        val callableTask  = Callable {
            return@Callable httpClient.execute(httpGet)
        }
        return executers.submit(callableTask).get()
    }

    fun deleteObject(bucket: String, key: String): HttpResponse {
        val callableTask = Callable {
            val httpdelete = HttpDelete("$OBJECT_CRUD_PATH/$bucket/$key")
//        val filebody = FileBody(file, ContentType.DEFAULT_BINARY)
            val httpresponse: HttpResponse = httpClient.execute(httpdelete)
            return@Callable httpresponse
        }
        return executers.submit(callableTask).get()
    }

    fun appendObject(bucket: String, key: String, objectToPut: ObjectToPut): HttpResponse {
        val httpPut = HttpPut("$OBJECT_CRUD_PATH/$bucket/$key")
        val entity = if (objectToPut.length != null && objectToPut.contentType != null)
            InputStreamEntity(objectToPut.stream, objectToPut.length, ContentType.parse(objectToPut.contentType))
        else InputStreamEntity(objectToPut.stream)
        objectToPut.metaData?.forEach { (key, value) ->
            httpPut.addHeader(key, value)
        }
        httpPut.addHeader("Connection", "keep-alive")

//        val entity = ByteArrayEntity(data)
        httpPut.entity = entity
        val callableTask = Callable {
            val httpResponse = httpClient.execute(httpPut)
            return@Callable httpResponse
        }
        return executers.submit(callableTask).get()
    }

    fun getObjectMeta(bucket: String, key: String): HttpResponse {
        val callableTask = Callable {
            val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key/meta")
            val httpResponse = httpClient.execute(httpGet)
            return@Callable httpResponse
        }
        return executers.submit(callableTask).get()
    }

    fun getObjectMd5(bucket: String, key: String): HttpResponse {
        val callableTask = Callable {
            val httpGet = HttpGet("$OBJECT_CRUD_PATH/$bucket/$key/md5")
            val httpResponse = httpClient.execute(httpGet)
            return@Callable httpResponse
        }
        return executers.submit(callableTask).get()
    }

}

class ClientMultiThreaded() : Thread() {
    override fun run() {

        //Run method implementation . . . . . . . . . .
    }
}