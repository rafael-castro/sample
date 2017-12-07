package io.vertx.starter

import com.github.rjeschke.txtmark.Processor
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import java.util.Date
import java.util.stream.Collectors


class MainVerticle : AbstractVerticle() {

  companion object {
    @JvmStatic private val LOGGER = LoggerFactory.getLogger(MainVerticle::class.java)
    @JvmStatic private val SQL_CREATE_PAGES_TABLE: String = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)"
    @JvmStatic private val SQL_GET_PAGE: String = "select Id, Content from Pages where Name = ?"
    @JvmStatic private val SQL_CREATE_PAGE: String = "insert into Pages values (NULL, ?, ?)"
    @JvmStatic private val SQL_SAVE_PAGE: String = "update Pages set Content = ? where Id = ?"
    @JvmStatic private val SQL_ALL_PAGES: String = "select Name from Pages"
    @JvmStatic private val SQL_DELETE_PAGE: String = "delete from Pages where Id = ?"
    @JvmStatic private val EMPTY_PAGE_MARKDOWN: String = "# A new page\n\nFeel-free to write in Markdown!\n"
  }

  private lateinit var dbClient: JDBCClient

  private val templateEngine: FreeMarkerTemplateEngine = FreeMarkerTemplateEngine.create()

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>) {
    val steps = prepareDatabase().compose<Void>({ _ ->
        startHttpServer()
      })
    steps.setHandler(startFuture.completer())
  }

  private fun prepareDatabase(): Future<Void> {
    val future = Future.future<Void>()

    dbClient = JDBCClient.createShared(vertx, JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30))

    dbClient.getConnection({ar ->
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause())
      } else {
        val connection = ar.result()
        connection.execute(SQL_CREATE_PAGES_TABLE, {create ->
          connection.close()
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause())
            future.fail(create.cause())
          } else {
            future.complete()
          }
        })
      }
    })

    return future
  }

  private fun startHttpServer(): Future<Void> {
    val future = Future.future<Void>()

    val server = vertx.createHttpServer()

    val router = Router.router(vertx)

    router.get("/").handler(this::indexHandler)
    router.get("/wiki/:page").handler(this::pageRenderingHandler)
    router.post().handler(BodyHandler.create())
    router.post("/save").handler(this::pageUpdateHandler)
    router.post("/create").handler(this::pageCreateHandler)
    router.post("/delete").handler(this::pageDeletionHandler)

    server
      .requestHandler(router::accept)
      .listen(8080, {ar ->
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port 8080")
          future.complete()
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause())
          future.fail(ar.cause())
        }
      })

    return future
  }

  private fun indexHandler(context: RoutingContext) {
    dbClient.getConnection({car ->
      if (car.succeeded()) {
        val connection: SQLConnection = car.result()
        connection.query(SQL_ALL_PAGES, {res ->
          connection.close()

          if (res.succeeded()) {
            val pages: List<String> = res.result()
              .results
              .stream()
              .map({json -> json.getString(0)})
              .sorted()
              .collect(Collectors.toList())

            context.put("title", "Wiki home")
            context.put("pages", pages)
            templateEngine.render(context, "templates", "/index.ftl", {ar ->
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html")
                context.response().end(ar.result())
              } else {
                context.fail(ar.cause())
              }
            })
          } else {
            context.fail(res.cause())
          }
        })
      } else {
        context.fail(car.cause())
      }
    })
  }

  private fun pageRenderingHandler(context: RoutingContext) {
    val page = context.request().getParam("page")

    dbClient.getConnection({car ->
      if (car.succeeded()) {
        val connection = car.result()
        connection.queryWithParams(SQL_GET_PAGE, JsonArray().add(page), {fetch ->
          connection.close()
          if (fetch.succeeded()) {
            val row = fetch.result().results
              .stream()
              .findFirst()
              .orElseGet({JsonArray()
                .add(-1)
                .add(EMPTY_PAGE_MARKDOWN)})

            val id = row.getInteger(0)
            val rawContent = row.getString(1)

            with(context) {
              put("title", page)
              put("id", id)
              put("newPage", if (fetch.result().results.size == 0) "yes" else "no")
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
            context.fail(fetch.cause())
          }
        })
      } else {
        context.fail(car.cause())
      }
    })
  }

  private fun pageUpdateHandler(context: RoutingContext) {
    val id = context.request().getParam("id")
    val title = context.request().getParam("title")
    val markdown = context.request().getParam("markdown")
    val newPage = context.request().getParam("newPage") === "yes"

    dbClient.getConnection({car ->
      if (car.succeeded()) {
        val connection = car.result()
        val sql = if (newPage) SQL_CREATE_PAGE else SQL_SAVE_PAGE
        val params = JsonArray()
        if (newPage) {
          params.add(title).add(markdown)
        } else {
          params.add(markdown).add(id)
        }

        connection.updateWithParams(sql, params, {res ->
          connection.close()
          if (res.succeeded()) {
            with(context.response()){
              statusCode = 303
              putHeader("Location", "/wiki/" + title)
              end()
            }
          } else {
            context.fail(res.cause())
          }
        })
      } else {
        context.fail(car.cause())
      }
    })
  }

  private fun pageCreateHandler(context: RoutingContext) {
    val pageName = context.request().getParam("name")
    val location = if (pageName === null || pageName.isEmpty()) {
      "/"
    } else {
      "/wiki/" + pageName
    }

    with(context.response()){
      statusCode = 303
      putHeader("Location", location)
      end()
    }
  }

  private fun pageDeletionHandler(context: RoutingContext) {
    val id = context.request().getParam("id")
    dbClient.getConnection({car ->
      if (car.succeeded()) {
        val connection = car.result()
        connection.updateWithParams(SQL_DELETE_PAGE, JsonArray().add(id), {res ->
          connection.close()
          if (res.succeeded()) {
            with(context.response()){
              statusCode = 303
              putHeader("Location", "/")
              end()
            }
          } else {
            context.fail(res.cause())
          }
        })
      } else {
        context.fail(car.cause())
      }
    })
  }
}
