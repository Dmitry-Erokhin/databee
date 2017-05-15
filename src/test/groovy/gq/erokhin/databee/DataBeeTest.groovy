package gq.erokhin.databee

import spock.lang.Specification

import static java.lang.System.out

/**
 *  Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 *  15.05.17
 */
class DataBeeTest extends Specification {

    void setup() {
        out.println "Set up"
    }

    void cleanup() {
        out.println "cleanUp"
    }

    def "Sample test"() {
        setup:
        def x = 1

        when:
        x++

        then:
        x == 2
    }

}
