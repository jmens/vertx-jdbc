package de.jmens.example;

import static java.util.Arrays.asList;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import java.util.List;

public class MainVerticle extends AbstractVerticle {

	private JDBCClient client;
	private boolean forceDbError;

	public MainVerticle() {
		this.forceDbError = Main.forceDbError;
	}

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);

		final JsonObject config = new JsonObject()
				.put("url", "jdbc:h2:mem:test")
				.put("driverClass", "org.h2.Driver");

		client = JDBCClient.createNonShared(vertx, config);
	}

	@Override
	public void start() throws Exception {
		getSqlConnection().setHandler(connection -> {
			if (connection.succeeded()) {
				setupDatabaseSchema(connection.result())
						.compose(nothing -> insertUser(connection.result(), "Horst"))
						.compose(id -> insertPreferences(connection.result(), id, "List Items"))
						.compose(id -> insertPreferences(connection.result(), id, "Update Items"))
						.compose(id -> insertPreferencesFaulty(connection.result(), id, forceDbError))
						.compose(id -> insertPreferences(connection.result(), id, "Delete Items"))
						.compose(nothing -> dumpTables(connection.result()))
						.setHandler(nothing -> {
							if (nothing.succeeded()) {
								getVertx().eventBus().send("mainverticle", "Success");
							}
						});
			} else {
				vertx.eventBus().send("mainverticle", "Error");
			}
		});
	}

	private Future<Void> setupDatabaseSchema(SQLConnection connection) {

		System.out.println("Setup database schema");

		final Future<Void> future = Future.future();

		final List<String> ddl = asList(
				"create table user (id int primary key auto_increment, name varchar(255))",
				"create table preferences(id int primary key auto_increment, id_user int, name varchar(255))");

		connection.batch(ddl, result -> {
			if (result.succeeded()) {
				future.complete(null);
			} else {
				future.fail(result.cause());
			}
		});

		return future;
	}

	private Future<Integer> insertUser(SQLConnection connection, String name) {
		final Future<Integer> future = Future.future();

		System.out.println("Inserting user " + name);

		connection.update("insert into user values(DEFAULT, '" + name + "')", result -> {
			if (result.succeeded()) {
				future.complete(result.result().getKeys().getInteger(0));
			} else {
				handleFailure(connection, future, result.cause());
			}
		});

		return future;
	}

	private Future<Integer> insertPreferences(SQLConnection connection, int userId, String name) {
		final Future<Integer> future = Future.future();

		System.out.println("Inserting preferences " + name + " for user " + userId);

		connection.update("insert into preferences values(DEFAULT, " + userId + ", '" + name + "')", result -> {
			if (result.succeeded()) {
				future.complete(userId);
			} else {
				handleFailure(connection, future, result.cause());
			}
		});

		return future;
	}

	private Future<Integer> insertPreferencesFaulty(SQLConnection connection, int userId, boolean forceDbError) {
		final Future<Integer> future = Future.future();

		System.out.println("Inserting faulty preferences 'foo' for user " + userId);

		if (!forceDbError) {
			future.complete(userId);
			return future;
		}

		connection.update("insert into preferences values('id', " + userId + ", 'foo')", result -> {
			if (result.succeeded()) {
				future.complete(userId);
			} else {
				connection.rollback(rolledBack -> {
					if (rolledBack.succeeded()) {
						dumpTables(connection)
								.setHandler(dumped -> handleFailure(connection, future, future.cause()));
					} else {
						handleFailure(connection, future, future.cause());
					}
				});
			}
		});

		return future;
	}


	private Future<Void> dumpTables(SQLConnection connection) {

		System.out.println("Dumping tables");

		Future<Void> future = Future.future();

		connection.query("select u.id, u.name as username, p.name as pref from user u join preferences p on u.id=p.id_user", result -> {
			if (result.succeeded()) {
				System.out.println(result.result().getRows().toString());
				future.complete();
			} else {
				handleFailure(connection, future, result.cause());
			}
		});

		return future;
	}

	private Future<SQLConnection> getSqlConnection() {
		Future<SQLConnection> future = Future.future();

		System.out.println("Establishing database connection");

		client.getConnection(connection -> {
			if (connection.succeeded()) {
				connection.result().setAutoCommit(false, nothing -> {
					if (nothing.succeeded()) {
						future.complete(connection.result());
					} else {
						handleFailure(connection.result(), future, connection.cause());
					}
				});
			} else {
				handleFailure(null, future, connection.cause());
			}
		});

		return future;
	}

	private void handleFailure(SQLConnection connection, Future<?> future, Throwable cause) {
		future.fail(cause);
		vertx.eventBus().send("mainverticle", "Error");
		if (connection != null) {
			connection.close();
		}
	}
}
