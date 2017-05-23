package gq.erokhin.databee

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Array
import java.sql.Blob
import java.sql.Clob
import java.sql.NClob
import java.sql.Ref
import java.sql.RowId
import java.sql.SQLType
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 22.05.17
 */

class ResultSetWrapperTest extends Specification {

    @Unroll
    def "Should NOT allow to call #method(#params)"() {
        given:
        def resultSet = new ResultSetWrapper(null)

        when:
        resultSet."$method"(*params)

        then:
        thrown(UnsupportedOperationException)

        where:
        method                   | params
        "absolute"               | [0]
        "afterLast"              | []
        "beforeFirst"            | []
        "cancelRowUpdates"       | []
        "clearWarnings"          | []
        "close"                  | []
        "deleteRow"              | []
        "first"                  | []
        "getRef"                 | ["string"]
        "getRef"                 | [0]
        "getRowId"               | ["string"]
        "getRowId"               | [0]
        "getStatement"           | []
        "insertRow"              | []
        "last"                   | []
        "moveToCurrentRow"       | []
        "moveToInsertRow"        | []
        "next"                   | []
        "previous"               | []
        "refreshRow"             | []
        "relative"               | [0]
        "setFetchDirection"      | [0]
        "setFetchSize"           | [0]
        "updateArray"            | ["string", null as Array]
        "updateArray"            | [0, null as Array]
        "updateAsciiStream"      | ["string", null as InputStream]
        "updateAsciiStream"      | ["string", null as InputStream, 0]
        "updateAsciiStream"      | ["string", null as InputStream, 0L]
        "updateAsciiStream"      | [0, null as InputStream]
        "updateAsciiStream"      | [0, null as InputStream, 0]
        "updateAsciiStream"      | [0, null as InputStream, 0L]
        "updateBigDecimal"       | ["string", null as BigDecimal]
        "updateBigDecimal"       | [0, null as BigDecimal]
        "updateBinaryStream"     | ["string", null as InputStream]
        "updateBinaryStream"     | ["string", null as InputStream, 0]
        "updateBinaryStream"     | ["string", null as InputStream, 0L]
        "updateBinaryStream"     | [0, null as InputStream]
        "updateBinaryStream"     | [0, null as InputStream, 0]
        "updateBinaryStream"     | [0, null as InputStream, 0L]
        "updateBlob"             | ["string", null as InputStream]
        "updateBlob"             | ["string", null as InputStream, 0L]
        "updateBlob"             | ["string", null as Blob]
        "updateBlob"             | [0, null as InputStream]
        "updateBlob"             | [0, null as InputStream, 0L]
        "updateBlob"             | [0, null as Blob]
        "updateBoolean"          | ["string", false]
        "updateBoolean"          | [0, false]
        "updateByte"             | ["string", 0 as byte]
        "updateByte"             | [0, 0 as byte]
        "updateBytes"            | ["string", null]
        "updateBytes"            | [0, null]
        "updateCharacterStream"  | ["string", null as Reader]
        "updateCharacterStream"  | ["string", null as Reader, 0]
        "updateCharacterStream"  | ["string", null as Reader, 0L]
        "updateCharacterStream"  | [0, null as Reader]
        "updateCharacterStream"  | [0, null as Reader, 0]
        "updateCharacterStream"  | [0, null as Reader, 0L]
        "updateClob"             | ["string", null as Reader]
        "updateClob"             | ["string", null as Reader, 0L]
        "updateClob"             | ["string", null as Clob]
        "updateClob"             | [0, null as Reader]
        "updateClob"             | [0, null as Reader, 0L]
        "updateClob"             | [0, null as Clob]
        "updateDate"             | ["string", null as java.sql.Date]
        "updateDate"             | [0, null as java.sql.Date]
        "updateDouble"           | ["string", 0d]
        "updateDouble"           | [0, 0d]
        "updateFloat"            | ["string", 0f]
        "updateFloat"            | [0, 0f]
        "updateInt"              | ["string", 0]
        "updateInt"              | [0, 0]
        "updateLong"             | ["string", 0L]
        "updateLong"             | [0, 0L]
        "updateNCharacterStream" | ["string", null as Reader]
        "updateNCharacterStream" | ["string", null as Reader, 0L]
        "updateNCharacterStream" | [0, null as Reader]
        "updateNCharacterStream" | [0, null as Reader, 0L]
        "updateNClob"            | ["string", null as Reader]
        "updateNClob"            | ["string", null as Reader, 0L]
        "updateNClob"            | ["string", null as NClob]
        "updateNClob"            | [0, null as Reader]
        "updateNClob"            | [0, null as Reader, 0L]
        "updateNClob"            | [0, null as NClob]
        "updateNString"          | ["string", "string"]
        "updateNString"          | [0, "string"]
        "updateNull"             | ["string"]
        "updateNull"             | [0]
        "updateObject"           | ["string", null as Object]
        "updateObject"           | ["string", null as Object, 0]
        "updateObject"           | ["string", null as Object, null as SQLType]
        "updateObject"           | ["string", null as Object, null as SQLType, 0]
        "updateObject"           | [0, null as Object]
        "updateObject"           | [0, null as Object, 0]
        "updateObject"           | [0, null as Object, null as SQLType]
        "updateObject"           | [0, null as Object, null as SQLType, 0]
        "updateRef"              | ["string", null as Ref]
        "updateRef"              | [0, null as Ref]
        "updateRow"              | []
        "updateRowId"            | ["string", null as RowId]
        "updateRowId"            | [0, null as RowId]
        "updateShort"            | ["string", 0 as short ]
        "updateShort"            | [0, 0 as short]
        "updateSQLXML"           | ["string", null as SQLXML]
        "updateSQLXML"           | [0, null as SQLXML]
        "updateString"           | ["string", "string"]
        "updateString"           | [0, "string"]
        "updateTime"             | ["string", null as Time]
        "updateTime"             | [0, null as Time]
        "updateTimestamp"        | ["string", null as Timestamp]
        "updateTimestamp"        | [0, null as Timestamp]
    }
}
