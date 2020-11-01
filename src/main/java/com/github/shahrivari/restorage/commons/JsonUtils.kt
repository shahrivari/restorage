package com.github.shahrivari.restorage.commons

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())


fun Any.toJson(): String =
        mapper.writeValueAsString(this)

inline fun <reified T> fromJson(json: String): T =
        mapper.readValue(json, T::class.java)