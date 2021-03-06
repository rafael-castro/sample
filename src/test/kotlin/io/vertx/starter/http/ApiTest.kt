package io.vertx.starter.http

import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.kotlin.core.net.JksOptions
import io.vertx.starter.database.WikiDatabaseVerticle
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner::class)
class ApiTest {

  private lateinit var vertx: Vertx
  private lateinit var webClient: WebClient

  @Before
  fun prepare(context: TestContext) {
    vertx = Vertx.vertx()

    val dbConf = JsonObject()
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4)

    vertx.deployVerticle(WikiDatabaseVerticle(), DeploymentOptions()
      .setConfig(dbConf), context.asyncAssertSuccess())

    vertx.deployVerticle(HttpServerVerticle(), context.asyncAssertSuccess())

    webClient = WebClient.create(vertx, WebClientOptions().apply {
      defaultHost = "localhost"
      defaultPort = 8080
      isSsl = true
      trustOptions = JksOptions().apply {
        path = "server-keystore.jks"
        password = "secret"
      }
    })
  }

  @After
  fun finish(context: TestContext) {
    vertx.close(context.asyncAssertSuccess())
  }

  @Test
  fun play_with_api(context: TestContext) {
    val async = context.async()

    val page = JsonObject().apply {
      put("name", "Sample")
      put("markdown", "# A page")
    }

    val postResquest = Future.future<JsonObject>()
    webClient.post("/api/pages")
      .`as`(BodyCodec.jsonObject())
      .sendJsonObject(page, {ar ->
        if (ar.succeeded()) {
          val postResponse = ar.result()
          postResquest.complete(postResponse.body())
        } else {
          context.fail(ar.cause())
        }
      })

    val getRequest = Future.future<JsonObject>()
    postResquest.compose({h ->
      webClient.get("/api/pages")
        .`as`(BodyCodec.jsonObject())
        .send({ar ->
          if (ar.succeeded()) {
            val getResponse = ar.result()
            getRequest.complete(getResponse.body())
          } else {
            context.fail(ar.cause())
          }
        })
    }, getRequest)

    val putRequest = Future.future<JsonObject>()
    getRequest.compose({response ->
      val array = response.getJsonArray("pages")
      context.assertEquals(1, array.size())
      context.assertEquals(0, array.getJsonObject(0).getInteger("id"))
      webClient.put("/api/pages/0")
        .`as`(BodyCodec.jsonObject())
        .sendJsonObject(JsonObject()
          .put("id", 0)
          .put("markdown", "Oh Yeah!"), {ar ->
          if (ar.succeeded()) {
            val putResponse = ar.result()
            putRequest.complete(putResponse.body())
          } else {
            context.fail(ar.cause())
          }
        })
    }, putRequest)

    val deleteRequest = Future.future<JsonObject>()
    putRequest.compose({response ->
      context.assertTrue(response.getBoolean("success"))
      webClient.delete("/api/pages/0")
        .`as`(BodyCodec.jsonObject())
        .send({ar ->
          if (ar.succeeded()) {
            val delResponse = ar.result()
            deleteRequest.complete(delResponse.body())
          } else {
            context.fail(ar.cause())
          }
        })
    }, deleteRequest)

    deleteRequest.compose<Future<JsonObject>>({response ->
      context.assertTrue(response.getBoolean("success"))
      async.complete()
    }, Future.failedFuture("Oh?"))
  }
}