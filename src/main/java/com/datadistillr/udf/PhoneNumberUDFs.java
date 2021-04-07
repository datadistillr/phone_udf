package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.codehaus.janino.Java;

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
}
