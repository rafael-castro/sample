package io.vertx.starter.database

import io.vertx.codegen.annotations.Fluent
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import javax.annotation.processing.SupportedSourceVersion
import javax.lang.model.SourceVersion

@ProxyGen
@SupportedSourceVersion(SourceVersion.RELEASE_8)
interface WikiDatabaseService {

  @Fluent
  fun fetchAllPagesData(resultHandler: Handler<AsyncResult<List<JsonObject>>>): WikiDatabaseService

  @Fluent
  fun fetchAllPages(resultHandler: Handler<AsyncResult<JsonArray>>): WikiDatabaseService

  @Fluent
  fun fetchPage(name: String, resultHandler: Handler<AsyncResult<JsonObject>>): WikiDatabaseService

  @Fluent
  fun createPage(title: String, markdown: String, resultHandler: Handler<AsyncResult<Void>>): WikiDatabaseService

  @Fluent
  fun savePage(id: Int, markdown: String, resultHandler: Handler<AsyncResult<Void>>): WikiDatabaseService

  @Fluent
  fun deletePage(id: Int, resultHandler: Handler<AsyncResult<Void>>): WikiDatabaseService

}
