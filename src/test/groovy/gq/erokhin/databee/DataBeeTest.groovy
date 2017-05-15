package gq.erokhin.databee

import groovy.sql.Sql
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

import static gq.erokhin.databee.DataBeeTestUtils.DB_URL
import static gq.erokhin.databee.DataBeeTestUtils.SAMPLE_ROW_COUNT

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

    def "Sample test"() {
        given:
        def data

        when:
        data = new Sql(conn).rows("SELECT count(*) AS count FROM test")

        then:
        data[0].count == SAMPLE_ROW_COUNT
    }


    def "Should feed data through flux"() {
        given:
        def flux = DataBee.of(conn)
                .query('SELECT * FROM test')
                .mapper({ it.getString('data') })
                .flux()

        when:
        def data = flux.collectList().block()

        then:
        data.size() == SAMPLE_ROW_COUNT
        data == [1..SAMPLE_ROW_COUNT].collect { "Test data #$it" }
    }
}
