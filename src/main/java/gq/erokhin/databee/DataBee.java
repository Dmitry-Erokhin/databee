package gq.erokhin.databee;

import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

import reactor.core.publisher.Flux;

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 14/05/2017
 */
public final class DataBee<T> {
    private final Connection connection;
    private String query;
    private Function<ResultSet, T> mapper;
    private int fetchSize;
    private Supplier<Boolean> condition;
    private Duration interval;

    private volatile boolean active;
    private Boolean wasAutoCommit;

    private DataBee(final Connection connection) {
        try {
            if (connection.isClosed()) {
                throw new IllegalStateException("Can not operate on closed connection");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can not check if connection is closed", e);
        }
        this.connection = connection;
        this.active = false;
    }

    public static DataBee of(final Connection connection) {
        return new DataBee(Objects.requireNonNull(connection));
    }

    public DataBee<T> query(final String query) {
        this.query = Objects.requireNonNull(query);
        if (query.isEmpty()) {
            throw new IllegalArgumentException("Can not proceed – query is empty");
        }
        return this;
    }

    public DataBee<T> mapper(final Function<ResultSet, T> mapper) {
        this.mapper = Objects.requireNonNull(mapper);
        return this;
    }

    public DataBee<T> fetchSize(final int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public DataBee<T> repeatWhile(final Supplier<Boolean> condition) {
        return this.repeatWhile(condition, Duration.ZERO);
    }

    public DataBee<T> repeatWhile(final Supplier<Boolean> condition, final Duration interval) {
        this.condition = Objects.requireNonNull(condition);
        this.interval = Objects.requireNonNull(interval);
        if (interval.isNegative()) {
            throw new IllegalArgumentException("Can not proceed – interval is negative");
        }
        return this;
    }

    public Flux<T> flux() throws SQLException {
        assertCanBeActivated();
        active = true;

        final Statement statement = createStatement();

        return Flux.create(sink -> {
                    final Consumer<ResultSet> rsConsumer = rs -> {
                        sink.next(mapper.apply(rs));
                        try {
                            if (rs.isAfterLast()) { //Finish at the end on result set
                                sink.complete();
                            }
                        } catch (final SQLException e) {
                            sink.error(e);
                        }
                    };

                    final ResultSetEmitter emitter = new ResultSetEmitter(statement, query, rsConsumer);

                    sink.onRequest(n -> {
                                try {
                                    emitter.emmitResults(n);
                                } catch (final SQLException e) {
                                    sink.error(e);
                                }
                            }
                    );

                    sink.onDispose(() -> {
                        try {
                            connection.close();
                        } catch (final SQLException e) {//TODO: #6 logging
                            System.err.println("Could not properly close connection. Autocommit would not be restored.");
                            e.printStackTrace();
                            return;
                        }

                        if (wasAutoCommit != null && wasAutoCommit) {
                            try {
                                connection.setAutoCommit(true);
                            } catch (final SQLException e) {//TODO: #6 logging
                                System.err.println("Could not set connection autocommit state back to true");
                                e.printStackTrace();
                            }
                        }
                    });
                }
        );

    }

    private void assertCanBeActivated() {
        if (query == null) {
            throw new IllegalStateException("Can not proceed – query was not set");
        }

        if (mapper == null) {
            throw new IllegalStateException("Can not proceed – mapper was not set");
        }

        if (active) {
            throw new IllegalStateException("Can not create more then one stream from give data bee");
        }
    }

    private Statement createStatement() throws SQLException {
        final Statement statement;
        try {
            if (fetchSize > 0) {
                wasAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                statement = connection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                statement.setFetchSize(fetchSize);
            } else {
                statement = connection.createStatement();
            }
        } catch (final SQLException e) {
            e.printStackTrace(); //TODO #6 – add proper logging
            throw e;
        }
        return statement;
    }


}