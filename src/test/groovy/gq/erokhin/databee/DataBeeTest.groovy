package gq.erokhin.databee

import groovy.sql.Sql
import spock.lang.Specification

/**
 *  Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 *  15.05.17
 */
class DataBeeTest extends Specification {
    def static DB_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
    def static SAMPLE_ROW_COUNT = 1000;
    private Sql sql

    void setup() {
        sql = Sql.newInstance(DB_URL)
        sql.execute('CREATE TABLE test (id SERIAL PRIMARY KEY, data TEXT)')
        sql.withBatch { stmt ->
            SAMPLE_ROW_COUNT.times {
                stmt.addBatch("INSERT INTO test(data) VALUES('test data #$it')")
            }
        }
    }

    void cleanup() {
    }

    def "Sample test"() {
        setup:
        def data

        when:
        data = sql.rows("SELECT count(*) AS count FROM test")

        then:
        data[0].count == SAMPLE_ROW_COUNT
    }

}
