package com.datadistillr.udf;

import com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
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

import java.util.Locale;

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

  @Test
  public void testGetNationalNumber() throws RpcException {
    String sql = "SELECT getNationalNumber('8432158473') AS num1, " +
      "getNationalNumber('(843) 215-8473') AS num2, " +
      "getNationalNumber('+49 69 920 39031') AS num3, " +
      "getNationalNumber('01 48 87 20 16', 'FR') AS num4 " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("num1", MinorType.BIGINT)
      .add("num2", MinorType.BIGINT)
      .add("num3", MinorType.BIGINT)
      .add("num4", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(8432158473L, 8432158473L, 6992039031L, 148872016L)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }


  @Test
  public void testFormatNumber() throws RpcException {
    String sql = "SELECT formatPhoneNumber('+49 69 920 39031', 'e164') AS num1, " +
      "formatPhoneNumber('+49 69 920 39031', 'national')  AS num2, " +
      "formatPhoneNumber('+49 69 920 39031', 'international') AS num3, " +
      "formatPhoneNumber('+49 69 920 39031', 'rfc3966')  AS num4 " +
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
      .addRow("+496992039031", "069 92039031", "+49 69 92039031", "tel:+49-69-92039031")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMatchNumber() throws RpcException {
    String sql = "SELECT is_phone_number_match('+49 69 920 39031', '+496992039031') AS num1, " +
      "is_phone_number_match('1(443) 111-2222', '4431112222')  AS num2, " +
      "is_phone_number_match(truncatePhoneNumber(convertAlphaCharactersInPhoneNumber('1-800-MICROSOFT')), '18006427676') AS num3, " +
      "is_phone_number_match('(443)111-2222', 'bob')  AS num4 " +
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
      .addRow(true, true, true, false)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGetCarrier() throws Exception {
    String sql = "SELECT getCarrier('+41 798765432') FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();
  }

  @Test
  public void testLocation() throws Exception {
    String sql = "SELECT geoLocatePhoneNumber('(410) 943-2101') as num1, " +
      "geoLocatePhoneNumber('bob') as num2, " +
      "geoLocatePhoneNumber('+49 800 910-8000') as num3, " +
      "geoLocatePhoneNumber('+49 69 21000002') as num4, " +
      "geoLocatePhoneNumber('(480) 834-8319') as num5 " +
      "FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("num1", MinorType.VARCHAR)
      .add("num2", MinorType.VARCHAR)
      .add("num3", MinorType.VARCHAR)
      .add("num4", MinorType.VARCHAR)
      .add("num5", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow( "Hurlock, MD", "Invalid Number", "Germany", "Frankfurt am Main", "Mesa, AZ")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);

  }

  @Test
  public void testCarrier() throws Exception{
    PhoneNumber swissMobileNumber =
      new PhoneNumber().setCountryCode(41).setNationalNumber(798765432L);
    PhoneNumberToCarrierMapper carrierMapper = PhoneNumberToCarrierMapper.getInstance();
// Outputs "Swisscom"
    System.out.println(carrierMapper.getNameForNumber(swissMobileNumber, Locale.ENGLISH));
    System.out.println(swissMobileNumber);
    PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
    PhoneNumber number = phoneUtil.parse("+41 798765432","US");

    System.out.println(number);
    System.out.println(carrierMapper.getNameForNumber(number, Locale.ENGLISH));

    PhoneNumber number2 = phoneUtil.parse("+1 (443)762-3286","US");

    System.out.println(number2);
    System.out.println(carrierMapper.getNameForNumber(number2, Locale.ENGLISH));
  }
}

