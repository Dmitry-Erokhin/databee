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
    private final Statement stmt;
    private final String query;

    private ResultSet resultSet;

    ResultSetEmitter(final Statement stmt, final String query, final Consumer<? super ResultSet> consumer) {
        this.consumer = consumer;
        this.stmt = stmt;
        this.query = query;
    }

    void emmitResults(final long n) throws SQLException {
        if (resultSet == null) {
            resultSet = stmt.executeQuery(query);
        }
        int i = 0;
        while (i++ < n && resultSet.next()) { //Order of statements is very important :)
            consumer.accept(resultSet);
        }
    }
}