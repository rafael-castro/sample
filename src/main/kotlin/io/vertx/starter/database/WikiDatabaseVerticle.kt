package io.vertx.starter.database

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.serviceproxy.ProxyHelper
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.IOException
import java.util.Properties

class WikiDatabaseVerticle: AbstractVerticle() {

  companion object {
    @JvmStatic private val LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle::class.java)
    @JvmStatic val CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url"
    @JvmStatic val CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class"
    @JvmStatic val CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size"
    @JvmStatic val CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file"
    @JvmStatic val CONFIG_WIKIDB_QUEUE = "wikidb.queue"
  }

  @Throws(IOException::class)
  private fun loadSqlQueries(): HashMap<SqlQuery, String> {
    val queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE)
    val queriesInputStream = if (queriesFile != null) {
      FileInputStream(queriesFile)
    } else {
      javaClass.getResourceAsStream("/db-queries.properties")
    }

    val queriesProps = Properties()
    queriesProps.load(queriesInputStream)
    queriesInputStream.close()

    val sqlQueries = HashMap<SqlQuery, String>()
    with (sqlQueries) {
      put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"))
      put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"))
      put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"))
      put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"))
      put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"))
      put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"))
    }

    return sqlQueries
  }

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>) {
    /*
     * Note: this uses blocking APIs, but data is small...
     */

    val sqlQueries = loadSqlQueries()

    val dbClient = JDBCClient.createShared(vertx, JsonObject()
      .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30))
    )

    WikiDatabaseService.create(dbClient, sqlQueries, Handler({ready ->
      if (ready.succeeded()) {
        ProxyHelper.registerService(WikiDatabaseService::class.java, vertx, ready.result(), CONFIG_WIKIDB_QUEUE)
        startFuture.complete()
      } else {
        startFuture.fail(ready.cause())
      }
    }))
  }
}
