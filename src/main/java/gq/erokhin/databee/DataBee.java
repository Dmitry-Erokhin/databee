package gq.erokhin.databee;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

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

    private volatile boolean running;

    private DataBee(final Connection connection) {
        this.connection = connection;
        running = false;
    }

    public static <T> DataBee<T> of(final Connection connection) {
        return new DataBee<>(connection);
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

    public Flux<T> flux() {
        checkState();

        if (running) {
            throw new IllegalStateException("Can not create more then one stream from give data bee");
        }
        running = true;
        return Flux.empty(); //TODO: add implementation
    }

    private void checkState() {
        if (query == null) {
            throw new IllegalStateException("Can not proceed – query was not set");
        }

        if (mapper == null) {
            throw new IllegalStateException("Can not proceed – mapper was not set");
        }
    }

}