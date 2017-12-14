package io.vertx.starter.database

import io.vertx.core.DeploymentOptions
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.kotlin.core.json.JsonObject
import io.vertx.serviceproxy.ServiceProxyBuilder
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner::class)
class WikiDatabaseServiceTest {
  private lateinit var vertx: Vertx
  private lateinit var service: WikiDatabaseService

  @Before
  @Throws(InterruptedException::class)
  fun prepare(context: TestContext) {
    vertx = Vertx.vertx()

    val conf = JsonObject()
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4)

    vertx.deployVerticle(WikiDatabaseVerticle(), DeploymentOptions().setConfig(conf), context.asyncAssertSuccess({ id ->
      val builder = ServiceProxyBuilder(vertx).setAddress(WikiDatabaseVerticle.CONFIG_WIKIDB_QUEUE)
      service = builder.build(WikiDatabaseService::class.java)
    }))
  }

  @After
  fun finish(context: TestContext) {
    vertx.close(context.asyncAssertSuccess())
  }

  @Test
  fun crud_operations(context: TestContext) {
    val async = context.async()

    service.createPage("Test", "Some content", context.asyncAssertSuccess({ v1 ->
      service.fetchPage("Test", context.asyncAssertSuccess({ json1 ->
        context.assertTrue(json1.getBoolean("found"))
        context.assertTrue(json1.containsKey("id"))
        context.assertEquals("Some content", json1.getString("rawContent"))

        service.savePage(json1.getInteger("id"), "Yo!", context.asyncAssertSuccess({ v2 ->
          service.fetchAllPages(context.asyncAssertSuccess( {array1 ->
            context.assertEquals(1, array1.size())

            service.fetchPage("Test", context.asyncAssertSuccess({json2 ->
              context.assertEquals("Yo!", json2.getString("rawContent"))

              service.deletePage(json1.getInteger("id"), Handler { v3 ->
                service.fetchAllPages(context.asyncAssertSuccess({array2 ->
                  context.assertTrue(array2.isEmpty)
                  async.complete()
                }))
              })
            }))
          }))
        }))
      }))
    }))
  }
}
