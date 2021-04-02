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

public class PhoneUdfTest extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testPhoneNumberType() throws RpcException {
    String sql = "SELECT getNumberType('8432158473', 'US') AS numType FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("numType", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("FIXED_LINE_OR_MOBILE")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testPhoneNumberTypeWithoutRegion() throws RpcException {
    String sql = "SELECT getNumberType('8432158473') AS numType FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("numType", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("FIXED_LINE_OR_MOBILE")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testValidPhoneNumber() throws RpcException {
    String sql = "SELECT isValidPhoneNumber('8432158473', 'US') AS num1, " +
      "isValidPhoneNumber('(843) 215-8473', 'US') AS num2, " +
      "isValidPhoneNumber('bob', 'US') AS num3, " +
      "isValidPhoneNumber('01 48 87 20 16', 'FR') AS num4 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("num1", MinorType.BIT)
      .add("num2", MinorType.BIT)
      .add("num3", MinorType.BIT)
      .add("num4", MinorType.BIT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(true, true, false, true)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testNormalizePhoneNumber() throws RpcException {
    String sql = "SELECT normalizePhoneNumber('8432158473') AS num1, " +
      "normalizePhoneNumber('(843) 215-8473') AS num2, " +
      "normalizePhoneNumber('+49 69 920 39031') AS num3, " +
      "normalizePhoneNumber('01 48 87 20 16') AS num4 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("num1", MinorType.VARCHAR)
      .add("num2", MinorType.VARCHAR)
      .add("num3", MinorType.VARCHAR)
      .add("num4", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("8432158473", "8432158473", "496992039031", "0148872016")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetCountryCode() throws RpcException {
    String sql = "SELECT getCountryCode('8432158473') AS num1, " +
      "getCountryCode('(843) 215-8473') AS num2, " +
      "getCountryCode('+49 69 920 39031') AS num3, " +
      "getCountryCode('01 48 87 20 16') AS num4 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("num1", MinorType.INT)
      .add("num2", MinorType.INT)
      .add("num3", MinorType.INT)
      .add("num4", MinorType.INT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(1,1,49,1)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
