package io.vertx.starter.http

import com.github.rjeschke.txtmark.Processor
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
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
import java.util.Date
import io.vertx.serviceproxy.ServiceProxyBuilder



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

}
