package io.vertx.starter.http

import com.github.rjeschke.txtmark.Processor
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.starter.database.WikiDatabaseService
import org.slf4j.LoggerFactory
import io.vertx.serviceproxy.ServiceProxyBuilder
import java.util.*
import java.util.stream.Collectors


class HttpServerVerticle : AbstractVerticle() {

  companion object {
    @JvmStatic private val LOGGER = LoggerFactory.getLogger(HttpServerVerticle::class.java)
    @JvmStatic private val EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n"
    @JvmStatic val CONFIG_HTTP_SERVER_PORT = "http.server.port"
    @JvmStatic val CONFIG_WIKIDB_QUEUE = "wikidb.queue"
  }

  private val templateEngine: FreeMarkerTemplateEngine = FreeMarkerTemplateEngine.create()

  private lateinit var wikiDbQueue: String

  private lateinit var dbService: WikiDatabaseService

  private lateinit var webClient: WebClient

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>){
    val server = vertx.createHttpServer()

    wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue")
    val builder = ServiceProxyBuilder(vertx).setAddress(wikiDbQueue)
    dbService = builder.build(WikiDatabaseService::class.java)
    webClient = WebClient.create(vertx, WebClientOptions()
      .setSsl(true)
      .setUserAgent("vert-x3"))

    val router = Router.router(vertx)
    router.get("/").handler(this::indexHandler)
    router.get("/wiki/:page").handler(this::pageRenderingHandler)
    router.post().handler(BodyHandler.create())
    router.post("/save").handler(this::pageUpdateHandler)
    router.post("/create").handler(this::pageCreateHandler)
    router.post("/delete").handler(this::pageDeletionHandler)
    router.post("/backup").handler(this::backupHandler)

    val apiRouter = Router.router(vertx)
    apiRouter.get("/pages").handler(this::apiRoot)
    apiRouter.get("/pages/:id").handler(this::apiGetPage)
    apiRouter.post().handler(BodyHandler.create())
    apiRouter.post("/pages").handler(this::apiCreatePage)
    apiRouter.put().handler(BodyHandler.create())
    apiRouter.put("/pages/:id").handler(this::apiUpdatePage)
    apiRouter.delete("/pages/:id").handler(this::apiDeletePage)
    router.mountSubRouter("/api", apiRouter)

    val portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080)
    server
      .requestHandler(router::accept)
      .listen(portNumber, {ar ->
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port $portNumber")
          startFuture.complete()
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause())
          startFuture.fail(ar.cause())
        }
      })
  }

  private fun indexHandler(context: RoutingContext) {
    dbService.fetchAllPages(Handler{ reply ->
        if (reply.succeeded()) {
          context.put("title", "Wiki home")
          context.put("pages", reply.result().list)
          templateEngine.render(context, "templates", "/index.ftl", { ar ->
            if (ar.succeeded()) {
              context.response().putHeader("Content-Type", "text/html")
              context.response().end(ar.result())
            } else {
              context.fail(ar.cause())
            }
          })
        } else {
          context.fail(reply.cause())
        }
      })
  }

  private fun pageRenderingHandler(context: RoutingContext) {
    val requestedPage = context.request().getParam("page")

    dbService.fetchPage(requestedPage, Handler{reply ->
        if (reply.succeeded()) {
          val payload = reply.result()
          val found = payload.getBoolean("found")
          val rawContent = payload.getString("rawContent", EMPTY_PAGE_MARKDOWN)
          with(context) {
            put("title", requestedPage)
            put("id", payload.getInteger("id", -1))
            put("newPage", if (found) "no" else "yes")
            put("rawContent", rawContent)
            put("content", Processor.process(rawContent))
            put("timestamp", Date().toString())
          }

          templateEngine.render(context, "templates", "/page.ftl", { ar ->
            if (ar.succeeded()) {
              context.response().putHeader("Content-Type", "text/html")
              context.response().end(ar.result())
            } else {
              context.fail(ar.cause())
            }
          })
        } else {
          context.fail(reply.cause())
        }
      })
  }

  private fun pageUpdateHandler(context: RoutingContext) {
    val title = context.request().getParam("title")
    val markdown = context.request().getParam("markdown")
    val id = Integer.valueOf(context.request().getParam("id"))

    val handler: Handler<AsyncResult<Void>> = Handler{reply ->
      if (reply.succeeded()) {
        context.response().statusCode = 303
        context.response().putHeader("Location", "/wiki/$title")
        context.response().end()
      } else {
        context.fail(reply.cause())
      }
    }

    if (context.request().getParam("newPage") === "yes") {
      dbService.createPage(title, markdown, handler)
    } else {
      dbService.savePage(id, markdown, handler)
    }
  }

  private fun pageCreateHandler(context: RoutingContext) {
    val pageName = context.request().getParam("name")
    val location = if (pageName.isNullOrEmpty()) { "/" } else { "/wiki/$pageName" }

    context.response().statusCode = 303
    context.response().putHeader("Location", location)
    context.response().end()
  }

  private fun pageDeletionHandler(context: RoutingContext) {
    val id = context.request().getParam("id")
    val request = JsonObject().put("id", id)
    val options = DeliveryOptions().addHeader("action", "delete-page")

    vertx
      .eventBus()
      .send<Any>(wikiDbQueue, request, options, {reply ->
        if (reply.succeeded()) {
          context.response().statusCode = 303
          context.response().putHeader("Location", "/")
          context.response().end()
        } else {
          context.fail(reply.cause())
        }
      })
  }

  private fun backupHandler(context: RoutingContext) {
    dbService.fetchAllPagesData(Handler({reply ->
      if (reply.succeeded()) {
        val filesObject = JsonObject()
        val gistPayload = JsonObject()
          .put("files", filesObject)
          .put("description", "A wiki backup")
          .put("public", true)

        reply
          .result()
          .forEach({page ->
            filesObject.put(page.getString("NAME"), JsonObject()
              .put("content", page.getString("CONTENT")))
          })

        webClient.post(443, "api.github", "/gists")
          .putHeader("Accept", "application/vnd.github.v3+json")
          .putHeader("Content-Type", "application/json")
          .`as`(BodyCodec.jsonObject())
          .sendJsonObject(gistPayload, {ar ->
            if (ar.succeeded()) {
              val response = ar.result()
              if (response.statusCode() == 201) {
                context.put("backup_gist_url", response.body().getString("html_url"))
                indexHandler(context)
              } else {
                val message = StringBuilder()
                  .append("Could not backup the wiki: ")
                  .append(response.statusMessage())
                val body = response.body()
                if (body != null) {
                  message
                    .append(System.getProperty("line.separator"))
                    .append(body.encodePrettily())
                }
                LOGGER.error(message.toString())
                context.fail(502)
              }
            } else {
              val err = ar.cause()
              LOGGER.error("HTTP Client error", err)
              context.fail(err)
            }
          })
      } else {
        context.fail(reply.cause())
      }
    }))
  }

  private fun apiRoot(context: RoutingContext) {
    dbService.fetchAllPagesData(Handler({ reply ->
      val response = JsonObject()
      if (reply.succeeded()) {
        val pages = reply.result()
          .stream()
          .map({obj ->
            JsonObject()
              .put("id", obj.getInteger("id"))
              .put("name", obj.getString("name"))
          })
          .collect(Collectors.toList())

        response
          .put("success", true)
          .put("pages", pages)

        with(context.response()) {
          statusCode = 200
          putHeader("Content-Type", "application/json")
          end(response.encode())
        }
      } else {
        response
          .put("success", false)
          .put("error", reply.cause().message)

        with(context.response()) {
          statusCode = 500
          putHeader("Content-Type", "application/json")
          end(response.encode())
        }
      }
    }))
  }

  private fun apiGetPage(context: RoutingContext) {
    val id = context.request().getParam("id").toInt()
    dbService.fetchPageById(id, Handler({reply ->
      val response = JsonObject()
      if (reply.succeeded()) {
        val dbObject = reply.result()

        if (dbObject.getBoolean("found")) {
          val payload = JsonObject()
            .put("name", dbObject.getString("name"))
            .put("id", dbObject.getInteger("id"))
            .put("markdown", dbObject.getString("content"))
            .put("html", Processor.process(dbObject.getString("content")))
          response
            .put("success", true)
            .put("page", payload)

          context.response().statusCode = 200
        } else {
          context.response().statusCode = 404
          response
            .put("success", false)
            .put("error", "There is no page with ID $id")
        }
      } else {
        response
          .put("success", false)
          .put("error", reply.cause().message)
        context.response().statusCode = 500
      }

      context.response().putHeader("Content-Type", "application/json")
      context.response().end(response.encode())
    }))
  }

  private fun apiCreatePage(context: RoutingContext) {
    val page = context.bodyAsJson

    if (!validateJsonPageDocument(context, page, "name", "markdown")) {
      return
    }

    dbService.createPage(page.getString("name"), page.getString("markdown"), Handler({reply ->
      if (reply.succeeded()) {
        context.response().statusCode = 201
        context.response().putHeader("Content-Type", "application/json")
        context.response().end(JsonObject().put("success", true).encode())
      } else {
        context.response().statusCode = 500
        context.response().putHeader("Content-Type", "application/json")
        context.response().end(JsonObject()
          .put("success", false)
          .put("error", reply.cause().message).encode())
      }
    }))
  }

  private fun apiUpdatePage(context: RoutingContext) {
    val id = context.request().getParam("id").toInt()
    val page = context.bodyAsJson
    if (!validateJsonPageDocument(context, page, "markdown")) {
      return
    }
    dbService.savePage(id, page.getString("markdown"), Handler({reply ->
      handlerSimpleDbReply(context, reply)
    }))
  }

  private fun apiDeletePage(context: RoutingContext) {
    val id = context.request().getParam("id").toInt()
    dbService.deletePage(id, Handler({reply ->
      handlerSimpleDbReply(context, reply)
    }))
  }

  private fun validateJsonPageDocument(context: RoutingContext, page: JsonObject, vararg expectedKeys: String): Boolean {
    if (!Arrays.stream(expectedKeys).allMatch(page::containsKey)) {
      LOGGER.error("Bad page creation JSON payload: ${page.encodePrettily()} from ${context.request().remoteAddress()}")
      context.response().statusCode = 400
      context.response().putHeader("Content-Type", "application/json")
      context.response().end(JsonObject()
        .put("success", false)
        .put("error", "Bad request payload")
        .encode())
      return false
    }
    return true
  }

  private fun handlerSimpleDbReply(context: RoutingContext, reply: AsyncResult<Void>) {
    if (reply.succeeded()) {
      context.response().statusCode = 200
      context.response().putHeader("Content-Type", "application/json")
      context.response().end(JsonObject().put("success", true).encode())
    } else {
      context.response().statusCode = 500
      context.response().putHeader("Content-Type", "application/json")
      context.response().end(JsonObject()
        .put("success", false)
        .put("error", reply.cause().message).encode())
    }
  }

}
