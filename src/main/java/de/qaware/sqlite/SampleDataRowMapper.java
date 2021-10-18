package de.qaware.sqlite;

import de.qaware.data.SampleDataAvro;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;


public class SampleDataRowMapper implements RowMapper<SampleDataAvro> {

    @Override
    public SampleDataAvro map(ResultSet rs, StatementContext ctx) throws SQLException {
        return SampleDataAvro.newBuilder()
                .setInt1(rs.getInt("int1"))
                .setInt2(rs.getInt("int2"))
                .setInt3(rs.getInt("int3"))
                .setInt4(rs.getInt("int4"))
                .setInt5(rs.getInt("int5"))
                .setString1(rs.getString("string1"))
                .setString2(rs.getString("string2"))
                .setString3(rs.getString("string3"))
                .setString4(rs.getString("string4"))
                .setString5(rs.getString("string5"))
                .build();
    }
}
