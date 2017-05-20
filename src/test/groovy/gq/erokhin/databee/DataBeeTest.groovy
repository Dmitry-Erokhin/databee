package gq.erokhin.databee

import groovy.sql.Sql
import reactor.core.publisher.Flux
import spock.lang.Specification

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
        def sql = new Sql(conn)
        sql.execute('DROP TABLE IF EXISTS test')
        conn.close()
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

    def "Should wrap errors"() {
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
}
