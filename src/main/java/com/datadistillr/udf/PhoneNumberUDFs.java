package com.datadistillr.udf;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;

public class PhoneNumberUDFs {

  @FunctionTemplate(name = "time_bucket_ns",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class TimeBucketNSFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputPhoneNumber;

    @Output
    VarCharHolder result;

    @Workspace
    PhoneNumberUtil phoneUtil;

    @Override
    public void setup() {
      phoneUtil = PhoneNumberUtil.getInstance();
    }

    @Override
    public void eval() {
      String phoneNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPhoneNumber);
      // Get the interval in milliseconds and convert to nanoseconds

    }
  }

}
