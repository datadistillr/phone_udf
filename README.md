# Drill Phone Number Utility Functions
This collection of functions provide various capabilities relating to phone numbers.

### Region Codes:
For all functions, there are two versions, one with a region code and one without.  This is only used if the number being parsed is not written in international format. The country calling code for the number in this case would be stored as that of the default region supplied.

## Functions
* `getNumberType(<phoneNumber>, <region_code>)`:  Returns the number type (toll free, mobile, land line etc.)  If the region code is omitted, it defaults to `US`.
* `isValidPhoneNumber(<phoneNumber>)` Returns true if the supplied character sequence is a valid phone number, false if not. This does not mean a number can be successfully 
  dialed, just that it is a valid phone number.
* `normalizePhoneNumber(<phoneNumber>)`: This function removes all non-digit characters from a phone number. 
* `formatPhoneNumber(<phoneNumber>, <format>)`:   Accepts a phone number and formats it in one of four possible formats which are: `e164`, `national`, `international`, `rfc3966`.