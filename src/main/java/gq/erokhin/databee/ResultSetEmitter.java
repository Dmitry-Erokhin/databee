package gq.erokhin.databee;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 14/05/2017
 */
class ResultSetEmitter {
    private final Consumer<? super ResultSet> consumer;
    private final Runnable finalizer;
    private final Statement stmt;
    private final String query;

    private ResultSet resultSet;
    private ResultSet wrapper;


    ResultSetEmitter(final Statement stmt,
                     final String query,
                     final Consumer<? super ResultSet> consumer,
                     Runnable finalizer) {
        this.consumer = consumer;
        this.stmt = stmt;
        this.query = query;
        this.finalizer = finalizer;
    }

    void emmitResults(final long n) throws SQLException {
        if (resultSet == null) {
            resultSet = stmt.executeQuery(query);
            wrapper = new ResultSetWrapper(resultSet);
        }
        int i = 0;
        boolean hasData;

        do {
            hasData = resultSet.next();
            if (hasData) {
                consumer.accept(wrapper);
            } else {
                finalizer.run();
            }

        } while (i++ < n && hasData); //To consume even empty result sets al least once
    }
}