# Drill Phone Number Utility Functions
This collection of functions provide various capabilities relating to phone numbers.

### Region Codes:
For most functions, there are two versions, one with a region code and one without.  This is only used if the number being parsed is not written in international format. The country calling code for the number in this case would be stored as that of the default region supplied.

## Functions
* `numberize(<phoneNumber>)` Converts phone numbers with alpha characters to solely numbers.

* `formatPhoneNumber(<phoneNumber>, <format>)`:  Accepts a phone number and formats it in one of four possible formats which are: `e164`, `national`, `international`, `rfc3966`.

* `getCarrier(<phoneNumber>)`: Returns the carrier of a given number if available.

* `getCountryCode(<phoneNumber>)`: Accepts a phone number and returns the country code for that number.  IE: 1 for US, 49 for Germany. It does not matter if the user included
  the country code or not in the input.  This function should not be used for geo-location as some countries have multiple country codes and some country codes are shared by
  multiple countries.

* `getLeadingZeroes(<phoneNumber>)`:  Returns the number of leading zeros in a phone number.

* `geoLocatePhoneNumber(<phoneNumber>)`:  Attempts to geolocate the phone number. If the number is not valid, will return `Invalid Number`.  The function will provide as much
  detail as it has, so you might just get a country name or you might get city and country.

* `getNationalNumber(<phoneNumber>, <region_code>)`: Returns the national component of a phone number.

* `getNumberType(<phoneNumber>, <region_code>)`:  Returns the number type (toll free, mobile, land line etc.)  If the region code is omitted, it defaults to `US`.

* `isPhoneNumberMatch(<phoneNumber1>, <phoneNumber2>)`: Returns true if the number is a match, false if not.  Note that the numbers do not need to be in the same format.  IE
  `4101234567` will match `+1 (410) 123-4567`.  

* `isValidPhoneNumber(<phoneNumber>)` Returns true if the supplied character sequence is a valid phone number, false if not. This does not mean a number can be successfully
  dialed, just that it is a valid phone number.

* `normalizePhoneNumber(<phoneNumber>)`: This function removes all non-digit characters from a phone number.

* `truncatePhoneNumber(<phoneNumber>)`: In some countries, extra numbers are ignored, so you can have a legitimate phone number like `1-800-MICROSOFT` where the extra numbers are
dropped.  This function will remove any extra characters after the last legal number in a phone number.

* `getAreaCodeFromCity(<city>)`: Accepts a city and returns its area code. If city is invalid or not found, returns 'XX'.
  - INCOMPLETE

* `getCoordsFromAreaCode(<areaCode>)`: Accepts an area code and returns its latitude and longitude pair as a LIST of type DOUBLE. If area code is invalid or not found, returns a LIST containing [0.0, 0.0].

* `getCountryFromAreaCode(<areaCode>)`: Accepts an area code and returns its related country. If area code is invalid or not found, returns 'XX'.

* `getLatitudeFromAreaCode(<areaCode>)`: Accepts an area code and returns its latitude coordinate as type DOUBLE. If area code is invalid or not found, returns 0.0.

* `getLongitudeFromAreaCode(<areaCode>)`: Accepts an area code and returns its longitude coordinate as type DOUBLE. If area code is invalid or not found, returns 0.0.

* `getBinaryFromAreaCode(<areaCode>)`: Accepts an area code and returns the binary point for its latitude/longitude coordinate pair. If area code is invalid or not found, returns an empty binary element.
