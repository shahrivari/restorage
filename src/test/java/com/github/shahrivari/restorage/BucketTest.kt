package com.github.shahrivari.restorage

import io.restassured.RestAssured.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BucketTest: ReStorageTest() {
    @BeforeEach
    private fun resetDefaultBucket() {
        if (get(EXISTS_DEFAULT_BUCKET_PATH).statusCode == 200)
            delete(DELETE_DEFAULT_BUCKET_PATH)
    }


    @Test
    fun `test bucket creation`() {
        post(CREATE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(200)
        get(EXISTS_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(200)
    }

    @Test
    fun `should fail on duplicate bucket`() {
        post(CREATE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(200)
        post(CREATE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(409)
    }

    @Test
    fun `should fail on existence of absent bucket`() {
        get(EXISTS_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(404)
    }

    @Test
    fun `test bucket deletion`() {
        post(CREATE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(200)
        delete(DELETE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(200)
    }

    @Test
    fun `should fail on absent bucket deletion`() {
        delete(DELETE_DEFAULT_BUCKET_PATH)
                .then()
                .assertThat()
                .statusCode(404)
    }

}