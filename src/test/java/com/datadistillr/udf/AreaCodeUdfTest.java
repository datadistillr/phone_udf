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

import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.test.rowSet.RowSetUtilities.binArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.doubleArray;


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
  public void testGetCoordsFromAreaCode() throws RpcException {
    String sql = "SELECT getCoordsFromAreaCode('985') AS coordsFromAreaCode1, " +
      "getCoordsFromAreaCode('   418 ') AS coordsFromAreaCode2, " +
      "getCoordsFromAreaCode('123*') AS coordsFromAreaCode3, " +
      "getCoordsFromAreaCode('') AS coordsFromAreaCode4, " +
      "getCoordsFromAreaCode('   ') AS coordsFromAreaCode5 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("coordsFromAreaCode1", MinorType.FLOAT8)
      .addArray("coordsFromAreaCode2", MinorType.FLOAT8)
      .addArray("coordsFromAreaCode3", MinorType.FLOAT8)
      .addArray("coordsFromAreaCode4", MinorType.FLOAT8)
      .addArray("coordsFromAreaCode5", MinorType.FLOAT8).build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(doubleArray(29.979313333333, -90.32739),
        doubleArray(47.215538085106, -71.384436170213),
        doubleArray(0.0, 0.0),
        doubleArray(0.0, 0.0),
        doubleArray(0.0, 0.0))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetCountryFromAreaCode() throws RpcException {
    String sql = "SELECT getCountryFromAreaCode('985') AS country1, " + "getCountryFromAreaCode('   418') AS country2, " + "getCountryFromAreaCode('123*') AS country3, " +
      "getCountryFromAreaCode('') AS country4, " + "getCountryFromAreaCode('  ') AS country5" + " FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("country1", MinorType.VARCHAR).add("country2", MinorType.VARCHAR).add("country3", MinorType.VARCHAR).add(
      "country4", MinorType.VARCHAR).add("country5", MinorType.VARCHAR).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow("US", "CA", "XX", "XX", "XX").build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetLatitudeFromAreaCode() throws RpcException {
    String sql = "SELECT getLatitudeFromAreaCode('985') AS latitude1, " + "getLatitudeFromAreaCode('  418') AS latitude2, " + "getLatitudeFromAreaCode('123*') AS latitude3, " +
      "getLatitudeFromAreaCode('') AS latitude4, " + "getLatitudeFromAreaCode('  ') AS latitude5" + " FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("latitude1", MinorType.FLOAT8).add("latitude2", MinorType.FLOAT8).add("latitude3", MinorType.FLOAT8).add(
      "latitude4", MinorType.FLOAT8).add("latitude5", MinorType.FLOAT8).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(29.979313333333, 47.215538085106, 0.0, 0.0, 0.0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetLongitudeFromAreaCode() throws RpcException {
    String sql = "SELECT getLongitudeFromAreaCode('985') AS longitude1, " + "getLongitudeFromAreaCode('  418') AS longitude2, " + "getLongitudeFromAreaCode('123*') AS " +
      "longitude3, " +
      "getLongitudeFromAreaCode('') AS longitude4, " + "getLongitudeFromAreaCode('   ') AS longitude5" + " FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("longitude1", MinorType.FLOAT8).add("longitude2", MinorType.FLOAT8).add("longitude3", MinorType.FLOAT8).add(
      "longitude4", MinorType.FLOAT8).add("longitude5", MinorType.FLOAT8).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(-90.32739, -71.384436170213, 0.0, 0.0, 0.0).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetBinaryFromAreaCode() throws RpcException {
    String sql = "SELECT getBinaryFromAreaCode('985') AS binaryFromAreaCode1, " + "getBinaryFromAreaCode('   418  ') AS binaryFromAreaCode2, " +
      "getBinaryFromAreaCode('123*') AS binaryFromAreaCode3, " + "getBinaryFromAreaCode('') AS binaryFromAreaCode4, " +
      "getBinaryFromAreaCode('  ') AS binaryFromAreaCode5 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();
    System.out.println(results);
    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("binaryFromAreaCode1", MinorType.VARBINARY)
      .add("binaryFromAreaCode2", MinorType.VARBINARY)
      .add("binaryFromAreaCode3", MinorType.VARBINARY)
      .add("binaryFromAreaCode4", MinorType.VARBINARY)
      .add("binaryFromAreaCode5", MinorType.VARBINARY)
      .build();

    byte[] binFromAreaCode1 = "\\x01\\x01\\x00\\x00\\x00e\\xC2/\\xF5\\xF3\\x94V\\xC0\\xB53SG\\xB4\\xFA=@".getBytes();
    byte[] binFromAreaCode2 = "\\x01\\x01\\x00\\x00\\x00\\xB7\\x9D*\\x9A\\x9A\\xD8Q\\xC0OI\\x81\\xC0\\x96\\x9BG@".getBytes();
    byte[] binFromAreaCode3 = "".getBytes();
    byte[] binFromAreaCode4 = "".getBytes();
    byte[] binFromAreaCode5 = "".getBytes();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(binArray(binFromAreaCode1, binFromAreaCode2, binFromAreaCode3, binFromAreaCode4, binFromAreaCode5))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
