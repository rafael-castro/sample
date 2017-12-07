package io.vertx.starter

import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future


class MainVerticle : AbstractVerticle() {

  @Throws(Exception::class)
  override fun start(startFuture: Future<Void>) {
    val dbVerticleDeployment = Future.future<String>()
    vertx.deployVerticle(WikiDatabaseVerticle(), dbVerticleDeployment.completer())

    dbVerticleDeployment.compose({id ->
      val httpVerticleDeployment = Future.future<String>()
      vertx.deployVerticle(
        "io.vertx.guides.wiki.HttpServerVerticle",
        DeploymentOptions().setInstances(2),
        httpVerticleDeployment.completer()
      )
      httpVerticleDeployment
    }).setHandler({ar ->
      if (ar.succeeded()) {
        startFuture.complete()
      } else {
        startFuture.fail(ar.cause())
      }
    })
  }
}
