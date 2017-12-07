package io.vertx.starter

import com.github.rjeschke.txtmark.Processor
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import io.vertx.ext.web.RoutingContext
import org.slf4j.LoggerFactory
import java.util.Date

class HttpServerVerticle : AbstractVerticle() {

  companion object {
    @JvmStatic private val LOGGER = LoggerFactory.getLogger(HttpServerVerticle::class.java)
    @JvmStatic private val EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n"
    @JvmStatic val CONFIG_HTTP_SERVER_PORT = "http.server.port"
    @JvmStatic val CONFIG_WIKIDB_QUEUE = "wikidb.queue"
  }

  private val templateEngine: FreeMarkerTemplateEngine = FreeMarkerTemplateEngine.create()

  private var wikiDbQueue = "wikidb.queue"

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>){
    wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue")

    val server = vertx.createHttpServer()

    val router = Router.router(vertx)
    router.get("/").handler(this::indexHandler)
    router.get("/wiki/:page").handler(this::pageRenderingHandler)
    router.post().handler(BodyHandler.create())
    router.post("/save").handler(this::pageUpdateHandler)
    router.post("/create").handler(this::pageCreateHandler)
    router.post("/delete").handler(this::pageDeletionHandler)

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
    val options = DeliveryOptions()
      .addHeader("action", "all-pages")

    vertx
      .eventBus()
      .send<Any>(wikiDbQueue, JsonObject(), options, { reply ->
        if (reply.succeeded()) {
          val body = reply.result().body() as JsonObject
          context.put("title", "Wiki home")
          context.put("pages", body.getJsonArray("pages").list)
          templateEngine.render(context, "templates", "/index.ftl", {ar ->
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
    val request = JsonObject().put("page", requestedPage)

    val options = DeliveryOptions()
      .addHeader("action", "get-page")

    vertx
      .eventBus()
      .send<Any>(wikiDbQueue, request, options, {reply ->
        if (reply.succeeded()) {
          val body = reply.result().body() as JsonObject

          val found = body.getBoolean("found")
          val rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN)
          with(context) {
            put("title", requestedPage)
            put("id", body.getInteger("id", -1))
            put("newPage", if (found) "no" else "yes")
            put("rawContent", rawContent)
            put("content", Processor.process(rawContent))
            put("timestamp", Date().toString())
          }

          templateEngine.render(context, "templates", "/page.ftl", {ar ->
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
    val action = if (context.request().getParam("newPage") === "yes") { "create-page" } else {"save-page"}
    val request = JsonObject()
      .put("id", context.request().getParam("id"))
      .put("title", title)
      .put("markdown", context.request().getParam("markdown"))

    val options = DeliveryOptions()
      .addHeader("action", action)

    vertx
      .eventBus()
      .send<Any>(wikiDbQueue, request, options, {reply ->
        if (reply.succeeded()) {
          context.response().statusCode = 303
          context.response().putHeader("Location", "/wiki/$title")
          context.response().end()
        } else {
          context.fail(reply.cause())
        }
      })
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

}
