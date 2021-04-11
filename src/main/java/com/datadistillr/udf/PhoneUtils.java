package com.datadistillr.udf;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.MatchType;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneUtils {
  private static final Logger logger = LoggerFactory.getLogger(PhoneUtils.class);

  public static String formatPhoneNumber(PhoneNumberUtil phoneUtil,
                                         String rawPhoneNumber,
                                         String formatString,
                                         String defaultRegion) {
    PhoneNumberFormat phoneNumberFormat;
    String formattedNumber;

    if (formatString.equalsIgnoreCase("e164")) {
      phoneNumberFormat = PhoneNumberUtil.PhoneNumberFormat.E164;
    } else if (formatString.equalsIgnoreCase("national")) {
      phoneNumberFormat = PhoneNumberUtil.PhoneNumberFormat.NATIONAL;
    } else if (formatString.equalsIgnoreCase("international")) {
      phoneNumberFormat = PhoneNumberUtil.PhoneNumberFormat.INTERNATIONAL;
    } else if (formatString.equalsIgnoreCase("rfc3966")) {
      phoneNumberFormat = PhoneNumberUtil.PhoneNumberFormat.RFC3966;
    } else {
        throw org.apache.drill.common.exceptions.UserException
          .functionError()
          .message("Invalid format string.  You must choose from e164, national, international, rfc3966")
          .build(logger);
    }

    try {
      if (Strings.isNullOrEmpty(defaultRegion)) {
        defaultRegion = "US";
      }

      PhoneNumber number = phoneUtil.parse(rawPhoneNumber, defaultRegion);
      formattedNumber = phoneUtil.format(number, phoneNumberFormat);
    } catch (com.google.i18n.phonenumbers.NumberParseException e) {
      // Invalid number, do nothing...
      return null;
    }
    return formattedNumber;
  }

  /**
   * This helper function compares two phone numbers and returns true if they are a match.  Note that
   * the Google phone number library has different classes of match which are: {@link com.google.i18n.phonenumbers.PhoneNumberUtil.MatchType}.
   * This method returns false if the match is a NO_MATCH or NOT_A_NUMBER.
   *
   * @param phoneUtil The phoneUtil object created in the setup() function of a Drill UDF
   * @param n1 The first phone number string
   * @param n2 The second phone number strin
   * @return True if the numbers match, false if not
   */
  public static boolean isMatch (PhoneNumberUtil phoneUtil, String n1, String n2) {
    MatchType match = phoneUtil.isNumberMatch(n1, n2);
    if (match == MatchType.NO_MATCH || match == MatchType.NOT_A_NUMBER) {
      return false;
    } else {
      return true;
    }
  }
}
