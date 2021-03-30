package com.datadistillr.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;

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
    VarCharHolder result;

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
        phoneUtil.getNumberType(number);
      } catch (com.google.i18n.phonenumbers.NumberParseException e) {
        e.printStackTrace();
      }

      // Get the interval in milliseconds and convert to nanoseconds

    }
  }

}
