package com.datadistillr.udf;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

public class AreaCodeUdfTest extends ClusterTest {
  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testGetAreaCodeFromCity() throws RpcException {
    String sql = "SELECT getAreaCodeFromCity('saginaw') AS areaCode1, " + "getAreaCodeFromCity('moose jaw  ') AS areaCode2, " + "getAreaCodeFromCity('') AS areaCode3, " +
      "getAreaCodeFromCity('  ') AS areaCode4, " + "getAreaCodeFromCity(' sitka  ') AS areaCode5, " + "getAreaCodeFromCity('#67') AS areaCode6 " + "FROM (VALUES" +
      "(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("areaCode1", MinorType.VARCHAR).add("areaCode2", MinorType.VARCHAR).add("areaCode3", MinorType.VARCHAR).add(
      "areaCode4", MinorType.VARCHAR).add("areaCode5", MinorType.VARCHAR).add("areaCode6", MinorType.VARCHAR).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow("989", "306", "XX", "XX", "907", "XX").build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetLatAndLong() throws RpcException {
    String sql = "SELECT getLatAndLong('985') AS latAndLong1, " + "getLatAndLong('   801') AS latAndLong2, " + "getLatAndLong('123*') AS latAndLong3, " +
      "getLatAndLong('') AS latAndLong4, " + "getLatAndLong('  ') AS latAndLong5" + " FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("latAndLong1", MinorType.VARCHAR).add("latAndLong2", MinorType.VARCHAR).add("latAndLong3", MinorType.VARCHAR).add(
      "latAndLong4", MinorType.VARCHAR).add("latAndLong5", MinorType.VARCHAR).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow("29.979313333333,-90.32739", "40.653856428571,-111.88019214286", "XX", "XX", "XX").build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
