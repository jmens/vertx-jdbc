package de.jmens.example;

import io.vertx.core.Vertx;

public class Main {

	public static final Vertx vertx = Vertx.vertx();

	public static final boolean forceDbError = false;

	public static void main(String[] args) {
		vertx.eventBus()
				.consumer("mainverticle")
				.handler(message -> {
					System.out.println("MainVerticle finished with:" + message.body());
					vertx.close();
				});

		vertx.deployVerticle(new MainVerticle());
	}
}
