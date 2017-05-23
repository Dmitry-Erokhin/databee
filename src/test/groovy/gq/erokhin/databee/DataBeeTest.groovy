package gq.erokhin.databee

import groovy.sql.Sql
import reactor.core.publisher.Flux
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

import static gq.erokhin.databee.DataBeeTestUtils.*

/**
 *  Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 *  15.05.17
 */
class DataBeeTest extends Specification {
    Connection conn

    void setup() {
        conn = DriverManager.getConnection(DB_URL)
        def sql = new Sql(conn)
        sql.execute('CREATE TABLE test (id SERIAL PRIMARY KEY, data TEXT)')
        sql.withBatch { stmt ->
            SAMPLE_ROW_COUNT.times {
                stmt.addBatch("INSERT INTO test(data) VALUES('Test data #$it')")
            }
        }
    }

    void cleanup() {
        Sql.newInstance(DB_URL).execute('DROP TABLE IF EXISTS test')
    }

    def "Should feed data"() {
        given:
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test')
                .mapper({ it.getString('data') })
                .flux()

        when:
        def data = flux.collectList().block(MAX_WAIT)

        then:
        data.size() == SAMPLE_ROW_COUNT
        data == (0..SAMPLE_ROW_COUNT - 1).collect({ "Test data #$it" })
    }

    def "Should produce correct data for empty result set"() {
        given:
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test WHERE 1=0')
                .mapper({ it })
                .flux()

        when:
        def data = flux.collectList().block(MAX_WAIT)

        then:
        data.size() == 0
    }

    def "Should wrap errors with handler"() {
        given:
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test WHERE 1/0 = 5')
                .mapper({ it })
                .flux()

        when:
        def data = flux.onErrorResume(SQLException.class, { Flux.just('Plan B') }).collectList().block(MAX_WAIT)

        then:
        data == ['Plan B']
    }

    def "Should NOT wrap errors without handler"() {
        given:
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test WHERE 1/0 = 5')
                .mapper({ it })
                .flux()

        when:
        flux.collectList().block(MAX_WAIT)

        then:
        Throwable e = thrown()
        e.getSuppressed().any { it.class.isAssignableFrom(SQLException.class) }
    }

    @Unroll
    def "Should close connection when #caseName"() {
        given:
        def flux = DataBee.of(conn)
                .query(sql)
                .mapper({ it.getString('data') })
                .flux()

        if (recover) {
            flux = flux.onErrorResume(SQLException.class, { Flux.just('Plan B') })
        }

        when:
        try {
            flux.collectList().block(MAX_WAIT)
        } catch (Throwable ignored) {
        }

        then:
        conn.isClosed()

        where:
        caseName                                | recover | sql
        "finishing feed"                        | true    | 'SELECT * FROM test'
        "empty result set returned"             | true    | 'SELECT * FROM test WHERE 1=0'
        "exception occurred (with recovery)"    | true    | 'SELECT * FROM test WHERE 1/0=5'
        "exception occurred (without recovery)" | false   | 'SELECT * FROM test WHERE 1/0=5'
    }


    def "Should wrap ResultSet for mapper"() {
        given:
        def result = ""
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test LIMIT 1')
                .mapper({ result = it.class.getName() })
                .flux()

        when:
        flux.collectList().block(MAX_WAIT)

        then:
        result == ResultSetWrapper.class.getName()

    }
}
