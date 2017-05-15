package gq.erokhin.databee

import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration

import static gq.erokhin.databee.DataBeeTestUtils.DB_URL

/**
 *  Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 *  15.05.17
 */
class DataBeeCreationTest extends Specification {
    Connection conn

    void setup() {
        conn = DriverManager.getConnection DB_URL
    }

    void cleanup() {
        if (!conn.isClosed()) {
            conn.close()
        }
    }

    def "Should NOT allow create flux for closed connection"() {
        when:
        conn.close()
        DataBee.of(conn).query("SELECT 1").flux()

        then:
        thrown(Exception)
    }


    def "Should NOT allow create flux for DataBee w/o query"() {
        when:
        DataBee.of(conn).flux()

        then:
        thrown(Exception)
    }

    def "Should NOT allow create flux for DataBee with empty query"() {
        when:
        DataBee.of(conn).query("").flux()

        then:
        thrown(Exception)
    }

    def "Should accept only positive intervals for recurrent setup"() {
        when:
        DataBee.of(conn).repeatWhile({ true }, Duration.ofDays(1).negated())

        then:
        thrown(Exception)
    }

    def "Should create flux for configured DataBee"() {
        when:
        def flux = DataBee.of(conn).query("SELECT 1").flux()

        then:
        noExceptionThrown()
        flux
    }
}
