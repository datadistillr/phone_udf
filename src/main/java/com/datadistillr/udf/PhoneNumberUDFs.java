package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
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
        e.printStackTrace();
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = numberType.getBytes().length;
      buffer.setBytes(0, numberType.getBytes());
    }
  }

}
