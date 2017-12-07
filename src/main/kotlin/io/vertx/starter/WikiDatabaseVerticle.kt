package io.vertx.starter

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.IOException
import java.util.Properties
import java.util.stream.Collectors

class WikiDatabaseVerticle: AbstractVerticle() {

  companion object {
    @JvmStatic private val LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle::class.java)
    @JvmStatic val CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url"
    @JvmStatic val CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class"
    @JvmStatic val CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size"
    @JvmStatic val CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file"
    @JvmStatic val CONFIG_WIKIDB_QUEUE = "wikidb.queue"
  }

  private val sqlQueries = HashMap<SqlQuery, String>()
  private lateinit var dbClient: JDBCClient

  @Throws(IOException::class)
  private fun loadSqlQueries() {
    val queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE)
    val queriesInputStream = if (queriesFile != null) {
      FileInputStream(queriesFile)
    } else {
      javaClass.getResourceAsStream("/db-queries.properties")
    }

    val queriesProps = Properties()
    queriesProps.load(queriesInputStream)
    queriesInputStream.close()

    with (sqlQueries) {
      put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"))
      put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"))
      put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"))
      put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"))
      put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"))
      put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"))
    }
  }

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>) {
    /*
     * Note: this uses blocking APIs, but data is small...
     */

    loadSqlQueries()

    dbClient = JDBCClient.createShared(vertx, JsonObject()
      .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30))
    )

    dbClient.getConnection({ar ->
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause())
        startFuture.fail(ar.cause())
      } else {
        val connection = ar.result()
        connection.execute(sqlQueries[SqlQuery.CREATE_PAGES_TABLE], { create ->
          connection.close()
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause())
            startFuture.fail(create.cause())
          } else {
            vertx
              .eventBus()
              .consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage)
            startFuture.complete()
          }
        })
      }
    })
  }

  private fun onMessage(message: Message<JsonObject>) {
    if (!message.headers().contains("action")) {
      LOGGER.error("No action header specified for message with headers {} and body {}"
        ,message.headers(), message.body().encodePrettily())
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal, "No action header specified")
      return
    }

    val action = message.headers().get("action")

    when (action){
      "all-pages" -> fetchAllPages(message)
      "get-page" -> fetchPage(message)
      "create-page" -> createPage(message)
      "save-page" -> savePage(message)
      "delete-page" -> deletePage(message)
      else -> message.fail(ErrorCodes.BAD_ACTION.ordinal, "Bad action: $action")
    }
  }

  private fun fetchAllPages(message: Message<JsonObject>) {
    dbClient.query(sqlQueries[SqlQuery.ALL_PAGES], { res ->
      if (res.succeeded()) {
        val pages = res.result()
          .results
          .stream()
          .map({json -> json.getString(0)})
          .sorted()
          .collect(Collectors.toList())
        message.reply(JsonObject().put("pages", JsonArray(pages)))
      } else {
        reportQueryError(message, res.cause())
      }
    })
  }

  private fun fetchPage(message: Message<JsonObject>) {
    val requestedPage = message.body().getString("page")
    val params = JsonArray().add(requestedPage)

    dbClient.queryWithParams(sqlQueries[SqlQuery.GET_PAGE], params, { fetch ->
      if (fetch.succeeded()) {
        val response = JsonObject()
        val resultSet = fetch.result()

        if (resultSet.numRows == 0) {
          response.put("found", false)
        } else {
          response.put("found", true)
          val row = resultSet.results[0]
          response.put("id", row.getInteger(0))
          response.put("rawContent", row.getString(1))
        }
        message.reply(response)
      } else {
        reportQueryError(message, fetch.cause())
      }
    })
  }

  private fun createPage(message: Message<JsonObject>) {
    val request = message.body()
    val data = JsonArray()
      .add(request.getString("title"))
      .add(request.getString("markdown"))

    dbClient.updateWithParams(sqlQueries[SqlQuery.CREATE_PAGE], data, { res ->
      if (res.succeeded()) {
        message.reply("ok")
      } else {
        reportQueryError(message, res.cause())
      }
    })
  }

  private fun savePage(message: Message<JsonObject>) {
    val request = message.body()
    val data = JsonArray()
      .add(request.getString("markdown"))
      .add(request.getString("id"))

    dbClient.updateWithParams(sqlQueries[SqlQuery.SAVE_PAGE], data, { res ->
      if (res.succeeded()) {
        message.reply("ok")
      } else {
        reportQueryError(message, res.cause())
      }
    })
  }

  private fun deletePage(message: Message<JsonObject>) {
    val data = JsonArray().add(message.body().getString("id"))

    dbClient.updateWithParams(sqlQueries[SqlQuery.DELETE_PAGE], data, { res ->
      if (res.succeeded()) {
        message.reply("ok")
      } else {
        reportQueryError(message, res.cause())
      }
    })
  }

  private fun reportQueryError(message: Message<JsonObject>, cause: Throwable){
    LOGGER.error("Database query error", cause)
    message.fail(ErrorCodes.DB_ERROR.ordinal, cause.message)
  }
}

enum class SqlQuery {
  CREATE_PAGES_TABLE,
  ALL_PAGES,
  GET_PAGE,
  CREATE_PAGE,
  SAVE_PAGE,
  DELETE_PAGE
}

enum class ErrorCodes {
  NO_ACTION_SPECIFIED,
  BAD_ACTION,
  DB_ERROR
}
