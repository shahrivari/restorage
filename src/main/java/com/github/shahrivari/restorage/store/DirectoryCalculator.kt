package com.github.shahrivari.restorage.store

import com.google.common.hash.Hashing
import java.io.File

object DirectoryCalculator {

    fun getTwoNestedLevels(rootDir: String, key: String): String {
        val sha2 = Hashing.sha256().hashString(key, Charsets.UTF_8).toString()
        val sep = File.separator
        return "$rootDir$sep${sha2.substring(0, 2)}$sep${sha2.substring(2, 4)}$sep${sha2.substring(4)}"
    }

    fun getOneNestedLevels(rootDir: String, key: String): String {
        val sha2 = Hashing.sha256().hashString(key, Charsets.UTF_8).toString()
        val sep = File.separator
        return "$rootDir$sep${sha2.substring(0, 3)}$sep${sha2.substring(3)}"
    }

}