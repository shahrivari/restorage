package com.github.shahrivari.restorage.store.rocks

import com.github.shahrivari.restorage.store.*
import com.github.shahrivari.restorage.store.fs.BucketInfo
import org.rocksdb.Options
import org.rocksdb.RocksDB
import java.io.InputStream
import java.util.*

class RocksStore(val path: String) : Store {

    val db: RocksDB

    init {
        RocksDB.loadLibrary()
        val options = Options().setCreateIfMissing(true)
        db = RocksDB.open(options, "path/to/db")
    }

    override fun createBucket(bucket: String): BucketInfo {
        TODO("Not yet implemented")
    }

    override fun getBucketInfo(bucket: String): Optional<BucketInfo> {
        TODO("Not yet implemented")
    }

    override fun deleteBucket(bucket: String) {
        TODO("Not yet implemented")
    }

    override fun put(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult {
        TODO("Not yet implemented")
    }

    override fun append(bucket: String, key: String, data: InputStream, meta: MetaData): PutResult {
        TODO("Not yet implemented")
    }

    override fun getMeta(bucket: String, key: String): MetaData {
        TODO("Not yet implemented")
    }

    override fun objectExists(bucket: String, key: String): Boolean {
        TODO("Not yet implemented")
    }

    override fun get(bucket: String, key: String, start: Long?, end: Long?): GetResult {
        TODO("Not yet implemented")
    }

    override fun delete(bucket: String, key: String): Long {
        TODO("Not yet implemented")
    }

    override fun computeMd5(bucket: String, key: String): String {
        TODO("Not yet implemented")
    }

    override fun generateHls(bucket: String, key: String): HlsCreationResult {
        TODO("Not yet implemented")
    }

    override fun getHlsFile(bucket: String, key: String, file: String): InputStream {
        TODO("Not yet implemented")
    }
}