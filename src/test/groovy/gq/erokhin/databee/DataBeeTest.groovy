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
        setup:
        def data

        when:
        data = new Sql(conn).rows("SELECT count(*) AS count FROM test")

        then:
        data[0].count == SAMPLE_ROW_COUNT
    }
}
