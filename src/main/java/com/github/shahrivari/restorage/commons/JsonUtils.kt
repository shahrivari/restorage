package com.github.shahrivari.restorage.commons

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

val jacksonMapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())


fun Any.toJson(): String =
        jacksonMapper.writeValueAsString(this)

inline fun <reified T> fromJson(json: String): T =
        jacksonMapper.readValue(json, T::class.java)