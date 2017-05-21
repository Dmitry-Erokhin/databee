package gq.erokhin.databee;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 14/05/2017
 */
public final class DataBee<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DataBee.class);

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

        LOG.debug("New bee was created");
    }

    public static DataBee of(final Connection connection) {
        return new DataBee(Objects.requireNonNull(connection));
    }

    public DataBee<T> query(final String query) {
        this.query = Objects.requireNonNull(query);
        if (query.isEmpty()) {
            throw new IllegalArgumentException("Can not proceed – query is empty");
        }

        LOG.debug("Added query: {}", query);
        return this;
    }

    public DataBee<T> mapper(final Function<ResultSet, T> mapper) {
        this.mapper = Objects.requireNonNull(mapper);

        LOG.debug("Added mapper: {}", mapper);
        return this;
    }

    public DataBee<T> fetchSize(final int fetchSize) {
        this.fetchSize = fetchSize;

        LOG.debug("Set fetch size: {}", fetchSize);
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

        LOG.debug("Set condition guarded polling with interval of {} ms", interval.toMillis());
        return this;
    }

    public Flux<T> flux() throws SQLException {
        assertCanBeActivated();
        final Statement statement = createStatement();
        active = true;

        LOG.debug("Bee activated");

        return Flux.create(sink -> {
                    final Consumer<ResultSet> rsConsumer = rs -> {
                        sink.next(mapper.apply(rs));
                    };

                    final ResultSetEmitter emitter = new ResultSetEmitter(statement, query, rsConsumer, sink::complete);

                    sink.onDispose(() -> {
                        try {
                            connection.close();
                            LOG.debug("Bee connections closed");
                        } catch (final SQLException e) {
                            LOG.error("Could not properly close connection. Autocommit would not be restored.", e);
                            return;
                        }

                        if (wasAutoCommit != null && wasAutoCommit) {
                            try {
                                connection.setAutoCommit(true);
                            } catch (final SQLException e) {
                                LOG.error("Could not set connection autocommit state back to true");
                            }
                        }
                    });


                    //FIXME: create issue or PR to Reactor
                    // In Reactor core (v. 3.0.7) calling this function lead to implicit initial request to source.
                    // IF this initialisation happens before sink.onDispose AND data flow finishing (which is an our
                    // case cause subscription is unlimited) it can not proper dispose because dispose method will
                    // not be specified by this time
                    sink.onRequest(n -> {
                                try {
                                    emitter.emmitResults(n);
                                } catch (final SQLException e) {
                                    sink.error(e);
                                }
                            }
                    );

                    LOG.debug("Publisher created");
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
            throw new IllegalStateException("Can not create more then one stream from given data bee");
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
            LOG.warn("Could not create statement", e);
            throw e;
        }
        return statement;
    }

}