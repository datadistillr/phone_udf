package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class PhoneNumberUDFs {

  @FunctionTemplate(names = {"get_number_type", "getNumberType"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GetNumberTypeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder countryCodeHolder;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String countryCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(countryCodeHolder);
      String numberType = "";
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,countryCode);
        numberType = phoneUtil.getNumberType(number).name();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        numberType = "INVALID NUMBER FOR " + countryCode + " REGION";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = numberType.getBytes().length;
      buffer.setBytes(0, numberType.getBytes());
    }
  }

  @FunctionTemplate(names = {"get_number_type", "getNumberType"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GetNumberTypeFunctionWithDefaultCountryCode implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String numberType = "";
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        numberType = phoneUtil.getNumberType(number).name();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        numberType = "INVALID NUMBER";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = numberType.getBytes().length;
      buffer.setBytes(0, numberType.getBytes());
    }
  }

  @FunctionTemplate(names = {"is_valid_phone_number", "isValidPhoneNumber"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isValidPhoneNumber implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder countryCodeHolder;

    @Output
    BitHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String countryCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(countryCodeHolder);
      boolean isValid = false;
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,countryCode);
        isValid = phoneUtil.isValidNumber(number);
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing ... number is not valid
      }

      if (isValid) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  /**
   * This function removes all non-digit characters from a phone number.
   * Usage:  SELECT normalizePhoneNumber( '(202) 456-5555') FROM...
   * Returns: 2024565555
   */
  @FunctionTemplate(names = {"normalize_phone_number", "normalizePhoneNumber"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class normalizePhoneNumber implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String normalizedNumber = com.google.i18n.phonenumbers.PhoneNumberUtil.normalizeDigitsOnly(phoneNumber);

      out.buffer = buffer;
      out.start = 0;
      out.end = normalizedNumber.getBytes().length;
      buffer.setBytes(0, normalizedNumber.getBytes());
    }
  }

  @FunctionTemplate(names = {"get_country_code", "getCountryCode"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryCode implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder countryCodeHolder;

    @Output
    IntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String countryCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(countryCodeHolder);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,countryCode);
        out.value = number.getCountryCode();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }

  @FunctionTemplate(names = {"get_country_code", "getCountryCode"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryCodeWithoutRegion implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    IntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        out.value = number.getCountryCode();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }


  @FunctionTemplate(names = {"get_leading_zeroes", "getLeadingZeroes"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLeadingZeroes implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder countryCodeHolder;

    @Output
    IntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String countryCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(countryCodeHolder);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,countryCode);
        out.value = number.getNumberOfLeadingZeros();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }

  @FunctionTemplate(names = {"get_leading_zeroes", "getLeadingZeroes"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLeadingZerosWithoutRegionCode implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    IntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        out.value = number.getNumberOfLeadingZeros();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }


  @FunctionTemplate(names = {"get_national_number", "getNationalNumber"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getNationalNumber implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder countryCodeHolder;

    @Output
    BigIntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String countryCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(countryCodeHolder);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,countryCode);
        out.value = number.getNationalNumber();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }

  @FunctionTemplate(names = {"get_national_number", "getNationalNumber"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getNationalNumberWithoutRegion implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    BigIntHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        out.value = number.getNationalNumber();
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }
    }
  }

  @FunctionTemplate(names = {"format_phone_number", "formatPhoneNumber"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class formatPhoneNumber implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder formatType;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String formatString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(formatType).toLowerCase();
      String formattedNumber = com.datadistillr.udf.PhoneUtils.formatPhoneNumber(phoneUtil, phoneNumber, formatString, null);

      if (formattedNumber != null) {
        out.buffer = buffer;
        out.start = 0;
        out.end = formattedNumber.getBytes().length;
        buffer.setBytes(0, formattedNumber.getBytes());
      }
    }
  }

  @FunctionTemplate(names = {"is_phone_number_match", "isPhoneNumberMatch"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isPhoneNumberMatchUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder number1;

    @Param
    VarCharHolder number2;

    @Output
    BitHolder out;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String n1 = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(number1);
      String n2 = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(number2);

      boolean isMatch = com.datadistillr.udf.PhoneUtils.isMatch(phoneUtil, n1, n2);
      out.value = (isMatch) ? 1 : 0;
    }
  }

  @FunctionTemplate(names = {"numberize", "convertAlphaCharactersInPhoneNumber", "convert_alpha_characters_in_phone_number"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class convertAlphaUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder number1;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      String rawNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(number1);
      String numberWithoutAlphaCharacters = com.google.i18n.phonenumbers.PhoneNumberUtil.convertAlphaCharactersInNumber(rawNumber);

      out.buffer = buffer;
      out.start = 0;
      out.end = numberWithoutAlphaCharacters.getBytes().length;
      buffer.setBytes(0, numberWithoutAlphaCharacters.getBytes());
    }
  }

  @FunctionTemplate(names = {"truncatePhoneNumber", "truncate_phone_number"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class truncatePhoneNumberUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);

      String truncatedNumber = "";
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        if (phoneUtil.truncateTooLongNumber(number)) {
          truncatedNumber = phoneUtil.format(number, com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat.E164 );
        }
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = truncatedNumber.getBytes().length;
      buffer.setBytes(0, truncatedNumber.getBytes());
    }
  }

  @FunctionTemplate(names = {"truncatePhoneNumber", "truncate_phone_number"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class truncatePhoneNumberWithRegionUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputPhoneNumber;

    @Param
    VarCharHolder regionCodeHolder;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String regionCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(regionCodeHolder);

      String truncatedNumber = "";
      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,regionCode);
        if (phoneUtil.truncateTooLongNumber(number)) {
          truncatedNumber = phoneUtil.format(number, com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat.E164 );
        }
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = truncatedNumber.getBytes().length;
      buffer.setBytes(0, truncatedNumber.getBytes());
    }
  }

  @FunctionTemplate(names = {"getCarrier", "get_carrier"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCarrierUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper mapper;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
      mapper = com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String carrier = "Unknown";

      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        carrier = mapper.getNameForNumber(number, java.util.Locale.ENGLISH);

        if (carrier.isEmpty()) {
          carrier = "Undefined";
        }

      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = carrier.getBytes().length;
      buffer.setBytes(0, carrier.getBytes());
    }
  }

  @FunctionTemplate(names = {"locatePhoneNumber", "locate_phone_number", "geoLocatePhoneNumber", "geolocate_phone_number"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class geolocatePhoneNumberUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.google.i18n.phonenumbers.PhoneNumberUtil phoneUtil;

    @Workspace
    com.google.i18n.phonenumbers.geocoding.PhoneNumberOfflineGeocoder geocoder;

    @Override
    public void setup() {
      phoneUtil = com.google.i18n.phonenumbers.PhoneNumberUtil.getInstance();
      geocoder = com.google.i18n.phonenumbers.geocoding.PhoneNumberOfflineGeocoder.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      String location;

      try {
        com.google.i18n.phonenumbers.Phonenumber.PhoneNumber number = phoneUtil.parse(phoneNumber,"US");
        location = geocoder.getDescriptionForNumber(number, java.util.Locale.ENGLISH);

        if (location.isEmpty()) {
          location = "Unknown";
        }

      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        // Do nothing...
        location = "Invalid Number";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = location.getBytes().length;
      buffer.setBytes(0, location.getBytes());
    }
  }

  @FunctionTemplate(names = {"getAreaCodeFromCity", "get_area_code_from_city"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getAreaCodeFromCityUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputCity;

    @Output
    VarCharHolder out;

    @Workspace
    com.datadistillr.udf.AreaCodeUtils areaCodeUtils;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
      areaCodeUtils = new AreaCodeUtils();
    }

    @Override
    public void eval() {
      String city = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputCity);
      city = city.trim();
      String result = areaCodeUtils.getAreaCodeFromCity(city);

      out.buffer = buffer;
      out.start = 0;
      out.end = result.getBytes().length;
      buffer.setBytes(0, result.getBytes());
    }
  }

  @FunctionTemplate(names = {"getLatAndLong", "get_lat_and_long"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLatAndLongUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputAreaCode;

    @Output
    VarCharHolder out;

    @Workspace
    com.datadistillr.udf.AreaCodeUtils areaCodeUtils;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
      areaCodeUtils = new AreaCodeUtils();
    }

    @Override
    public void eval() {
      String areaCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputAreaCode);
      areaCode = areaCode.trim();
      String result = areaCodeUtils.getLatAndLong(areaCode);

      out.buffer = buffer;
      out.start = 0;
      out.end = result.getBytes().length;
      buffer.setBytes(0, result.getBytes());
    }
  }

  @FunctionTemplate(names = {"getCountryFromAreaCode", "get_country_from_area_code"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryFromAreaCodeUDF implements DrillSimpleFunc {
    @Param
    VarCharHolder inputAreaCode;

    @Output
    VarCharHolder out;

    @Workspace
    com.datadistillr.udf.AreaCodeUtils areaCodeUtils;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
      areaCodeUtils = new AreaCodeUtils();
    }

    @Override
    public void eval() {
      String areaCode = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputAreaCode);
      areaCode = areaCode.trim();
      String result = areaCodeUtils.getCountryFromAreaCode(areaCode);

      out.buffer = buffer;
      out.start = 0;
      out.end = result.getBytes().length;
      buffer.setBytes(0, result.getBytes());
    }
  }
}
