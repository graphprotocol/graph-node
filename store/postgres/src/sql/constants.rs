use std::collections::HashSet;

use lazy_static::lazy_static;
use sqlparser::dialect::PostgreSqlDialect;

lazy_static! {
    pub(super) static ref ALLOWED_FUNCTIONS: HashSet<&'static str> = {
       vec![
            // Comparison Functions see https://www.postgresql.org/docs/14/functions-comparison.html#FUNCTIONS-COMPARISON-FUNC-TABLE
            "num_nonnulls", // Number of non-null arguments
            "num_nulls", // Number of null arguments

            // Mathematical Functions see https://www.postgresql.org/docs/14/functions-math.html#FUNCTIONS-MATH-FUNC-TABLE
            "abs", // Asolute value
            "cbrt", // Cube root
            "ceil", // Nearest integer greater than or equal to argument
            "ceiling", // Nearest integer greater than or equal to argument
            "degrees", // Converts radians to degrees
            "div", // Integer quotient of y/x (truncates towards zero)
            "exp", // Exponential (e raised to the given power)
            "factorial", // Factorial
            "floor", // Nearest integer less than or equal to argument
            "gcd", // Greatest common divisor (the largest positive number that divides both inputs with no remainder); returns 0 if both inputs are zero; available for integer, bigint, and numeric
            "lcm", // Least common multiple (the smallest strictly positive number that is an integral multiple of both inputs); returns 0 if either input is zero; available for integer, bigint, and numeric
            "ln", // Natural logarithm
            "log", // Base 10 logarithm
            "log10", // Base 10 logarithm (same as log)
            "mod", // Remainder of y/x; available for smallint, integer, bigint, and numeric
            "pi", // Approximate value of π
            "power", // a raised to the power of b
            "radians", // Converts degrees to radians
            "round", // Rounds to nearest integer. For numeric, ties are broken by rounding away from zero. For double precision, the tie-breaking behavior is platform dependent, but “round to nearest even” is the most common rule.
            "scale", // Scale of the argument (the number of decimal digits in the fractional part)
            "sign", // Sign of the argument (-1, 0, or +1)
            "sqrt", // Square root
            "trim_scale", // Reduces the value's scale (number of fractional decimal digits) by removing trailing zeroes
            "trunc", // Truncates to integer (towards zero)
            "width_bucket", // Returns the number of the bucket in which operand falls in a histogram having count equal-width buckets spanning the range low to high. Returns 0 or count+1 for an input outside that range.

            // Random Functions see https://www.postgresql.org/docs/14/functions-math.html#FUNCTIONS-MATH-RANDOM-TABLE
            "random", // Returns a random value in the range 0.0 <= x < 1.0
            "setseed", // Sets the seed for subsequent random() calls; argument must be between -1.0 and 1.0, inclusive

            // Trigonometric Functions see https://www.postgresql.org/docs/14/functions-math.html#FUNCTIONS-MATH-TRIG-TABLE
            "acos", // Arc cosine, result in radians
            "acosd", // Arc cosine, result in degrees
            "asin", // Arc sine, result in radians
            "asind", // Arc sine, result in degrees
            "atan", // Arc tangent, result in radians
            "atand", // Arc tangent, result in degrees
            "atan2", // Arc tangent of y/x, result in radians
            "atan2d", // Arc tangent of y/x, result in degrees
            "cos", // Cosine, argument in radians
            "cosd", // Cosine, argument in degrees
            "cot", // Cotangent, argument in radians
            "cotd", // Cotangent, argument in degrees
            "sin", // Sine, argument in radians
            "sind", // Sine, argument in degrees
            "tan", // Tangent, argument in radians
            "tand", // Tangent, argument in degrees

            // Hyperbolic Functions see https://www.postgresql.org/docs/14/functions-math.html#FUNCTIONS-MATH-HYPERBOLIC-TABLE
            "sinh", // Hyperbolic sine
            "cosh", // Hyperbolic cosine
            "tanh", // Hyperbolic tangent
            "asinh", // Inverse hyperbolic sine
            "acosh", // Inverse hyperbolic cosine
            "atanh", // Inverse hyperbolic tangent

            // String Functions see https://www.postgresql.org/docs/14/functions-string.html#FUNCTIONS-STRING-SQL
            "bit_length", // Number of bits in string
            "char_length", // Number of characters in string
            "character_length", // Synonym for char_length
            "lower", // Convert string to lower case
            "normalize", // Convert string to specified Unicode normalization form
            "octet_length", // Number of bytes in string
            "overlay", // Replace substring
            "position", // Location of specified substring
            "substring", // Extract substring
            "trim", // Remove leading and trailing characters
            "upper", // Convert string to upper case

            //Additional string functions see https://www.postgresql.org/docs/14/functions-string.html#FUNCTIONS-STRING-OTHER
            "ascii", // Convert first character to its numeric code
            "btrim", // Remove the longest string containing only characters from characters (a space by default) from the start and end of string
            "chr", // Convert integer to character
            "concat", // Concatenate strings
            "concat_ws", // Concatenate with separator
            "format", // Format arguments according to a format string
            "initcap", // Convert first letter of each word to upper case and the rest to lower case
            "left", // Extract substring
            "length", // Number of characters in string
            "lpad", // Pad string to length length by prepending the characters fill (a space by default)
            "ltrim", // Remove the longest string containing only characters from characters (a space by default) from the start of string
            "md5", // Compute MD5 hash
            "parse_ident", // Split qualified_identifier into an array of identifiers, removing any quoting of individual identifiers
            "quote_ident", // Returns the given string suitably quoted to be used as an identifier in an SQL statement string
            "quote_literal", // Returns the given string suitably quoted to be used as a string literal in an SQL statement string
            "quote_nullable", // Returns the given string suitably quoted to be used as a string literal in an SQL statement string; or, if the argument is null, returns NULL
            "regexp_match", // Returns captured substrings resulting from the first match of a POSIX regular expression to the string
            "regexp_matches", // Returns captured substrings resulting from the first match of a POSIX regular expression to the string, or multiple matches if the g flag is used
            "regexp_replace", // Replaces substrings resulting from the first match of a POSIX regular expression, or multiple substring matches if the g flag is used
            "regexp_split_to_array", // Splits string using a POSIX regular expression as the delimiter, producing an array of results
            "regexp_split_to_table", // Splits string using a POSIX regular expression as the delimiter, producing a set of results
            "repeat", // Repeats string the specified number of times
            "replace", // Replaces all occurrences in string of substring from with substring to
            "reverse", // Reverses the order of the characters in the string
            "right", // Extract substring
            "rpad", // Pad string to length length by appending the characters fill (a space by default)
            "rtrim", // Remove the longest string containing only characters from characters (a space by default) from the end of string
            "split_part", // Splits string at occurrences of delimiter and returns the n'th field (counting from one), or when n is negative, returns the |n|'th-from-last field
            "strpos", // Returns first starting index of the specified substring within string, or zero if it's not present
            "substr", // Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified
            "starts_with", // Returns true if string starts with prefix
            "string_to_array", // Splits the string at occurrences of delimiter and forms the resulting fields into a text array
            "string_to_table", // Splits the string at occurrences of delimiter and returns the resulting fields as a set of text rows
            "to_ascii", // Converts string to ASCII from another encoding, which may be identified by name or number
            "to_hex", // Converts the number to its equivalent hexadecimal representation
            "translate", // Replaces each character in string that matches a character in the from set with the corresponding character in the to set
            "unistr", // Evaluate escaped Unicode characters in the argument

            // Binary String Functions see https://www.postgresql.org/docs/14/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
            "bit_count", // Number of bits set in the argument
            "get_bit", // Extracts the n'th bit from string
            "get_byte", // Extracts the n'th byte from string
            "set_bit", // Sets the n'th bit in string to newvalue
            "set_byte", // Sets the n'th byte in string to newvalue
            "sha224", // Compute SHA-224 hash
            "sha256", // Compute SHA-256 hash
            "sha384", // Compute SHA-384 hash
            "sha512", // Compute SHA-512 hash

            // String conversion functions see https://www.postgresql.org/docs/14/functions-binarystring.html#FUNCTIONS-BINARYSTRING-CONVERSIONS
            "convert", // Converts a binary string representing text in encoding src_encoding to a binary string in encoding dest_encoding
            "convert_from", // Converts a binary string representing text in encoding src_encoding to text in the database encoding
            "convert_to", // Converts a text string (in the database encoding) to a binary string encoded in encoding dest_encoding
            "encode", // Encodes binary data into a textual representation
            "decode", // Decodes binary data from a textual representation

            // Formatting Functions see https://www.postgresql.org/docs/14/functions-formatting.html#FUNCTIONS-FORMATTING-TABLE
            "to_char", // Converts number to a string according to the given format
            "to_date", // Converts string to date
            "to_number", // Converts string to number
            "to_timestamp", // Converts string to timestamp with time zone

            // Date/Time Functions see https://www.postgresql.org/docs/14/functions-datetime.html
            "age", // Subtract arguments, producing a “symbolic” result that uses years and months, rather than just days
            "clock_timestamp", // Current date and time (changes during statement execution)
            "current_date", // Current date
            "current_time", // Current time of day
            "current_timestamp", // Current date and time (start of current transaction)
            "date_bin", // Bin input into specified interval aligned with specified origin
            "date_part", // Get subfield (equivalent to extract)
            "date_trunc", // Truncate to specified precision
            "extract", // Get subfield
            "isfinite", // Test for finite date (not +/-infinity)
            "justify_days", // Adjust interval so 30-day time periods are represented as months
            "justify_hours", // Adjust interval so 24-hour time periods are represented as days
            "justify_interval", // Adjust interval using justify_days and justify_hours, with additional sign adjustments
            "localtime", // Current time of day
            "localtimestamp", // Current date and time (start of current transaction)
            "make_date", // Create date from year, month and day fields (negative years signify BC)
            "make_interval", // Create interval from years, months, weeks, days, hours, minutes and seconds fields, each of which can default to zero
            "make_time", // Create time from hour, minute and seconds fields
            "make_timestamp", // Create timestamp from year, month, day, hour, minute and seconds fields (negative years signify BC)
            "make_timestamptz", // Create timestamp with time zone from year, month, day, hour, minute and seconds fields (negative years signify BC).
            "now", // Current date and time (start of current transaction)
            "statement_timestamp", // Current date and time (start of current statement)
            "timeofday", // Current date and time (like clock_timestamp, but as a text string)
            "transaction_timestamp", // Current date and time (start of current transaction)

            // Enum support functions see https://www.postgresql.org/docs/14/functions-enum.html#FUNCTIONS-ENUM-SUPPORT
            "enum_first", // Returns the first value of an enum type
            "enum_last", // Returns the last value of an enum type
            "enum_range", // Returns a range of values of an enum type

            // Geometric Functions see https://www.postgresql.org/docs/14/functions-geometry.html
            "area", // Computes area
            "center", // Computes center point
            "diagonal", // Extracts box's diagonal as a line segment (same as lseg(box))
            "diameter", // Computes diameter of circle
            "height", // Computes vertical size of box
            "isclosed", // Is path closed?
            "isopen", // Is path open?
            "length", // Computes the total length
            "npoints", // Returns the number of points
            "pclose", // Converts path to closed form
            "popen", // Converts path to open form
            "radius", // Computes radius of circle
            "slope", // Computes slope of a line drawn through the two points
            "width", // Computes horizontal size of box

            // Geometric Type Conversion Functions see https://www.postgresql.org/docs/14/functions-geometry.html
            "box", // Convert to a box
            "circle", // Convert to a circle
            "line", // Convert to a line
            "lseg", // Convert to a line segment
            "path", // Convert to a path
            "point", // Convert to a point
            "polygon", // Convert to a polygon

            // IP Address Functions see https://www.postgresql.org/docs/14/functions-net.html
            "abbrev", // Creates an abbreviated display format as text
            "broadcast", // Computes the broadcast address for the address's network
            "family", // Returns the address's family: 4 for IPv4, 6 for IPv6
            "host", // Returns the IP address as text, ignoring the netmask
            "hostmask", // Computes the host mask for the address's network
            "inet_merge", // Computes the smallest network that includes both of the given networks
            "inet_same_family", // Tests whether the addresses belong to the same IP family
            "masklen", // Returns the netmask length in bits
            "netmask", // Computes the network mask for the address's network
            "network", // Returns the network part of the address, zeroing out whatever is to the right of the netmask
            "set_masklen", // Sets the netmask length for an inet value. The address part does not change
            "text", // Returns the unabbreviated IP address and netmask length as text

            // MAC Address Functions see https://www.postgresql.org/docs/14/functions-net.html#MACADDR-FUNCTIONS-TABLE
            "macaddr8_set7bit", //Sets the 7th bit of the address to one, creating what is known as modified EUI-64, for inclusion in an IPv6 address.

            // Text Search Functions see https://www.postgresql.org/docs/14/functions-textsearch.html
            "array_to_tsvector", // Converts an array of lexemes to a tsvector
            "get_current_ts_config", // Returns the OID of the current default text search configuration (as set by default_text_search_config)
            "numnode", // Returns the number of lexemes plus operators in the tsquery
            "plainto_tsquery", // Converts text to a tsquery, normalizing words according to the specified or default configuration.
            "phraseto_tsquery", // Converts text to a tsquery, normalizing words according to the specified or default configuration.
            "websearch_to_tsquery", // Converts text to a tsquery, normalizing words according to the specified or default configuration.
            "querytree", // Produces a representation of the indexable portion of a tsquery. A result that is empty or just T indicates a non-indexable query.
            "setweight", // Assigns the specified weight to each element of the vector.
            "strip", // Removes positions and weights from the tsvector.
            "to_tsquery", // Converts text to a tsquery, normalizing words according to the specified or default configuration.
            "to_tsvector", // Converts text to a tsvector, normalizing words according to the specified or default configuration.
            "json_to_tsvector", // Selects each item in the JSON document that is requested by the filter and converts each one to a tsvector, normalizing words according to the specified or default configuration.
            "jsonb_to_tsvector",// Selects each item in the JSON document that is requested by the filter and converts each one to a tsvector, normalizing words according to the specified or default configuration.
            "ts_delete", // Removes any occurrence of the given lexeme from the vector.
            "ts_filter", // Selects only elements with the given weights from the vector.
            "ts_headline", // Displays, in an abbreviated form, the match(es) for the query in the document, which must be raw text not a tsvector.
            "ts_rank", // Computes a score showing how well the vector matches the query. See Section 12.3.3 for details.
            "ts_rank_cd", // Computes a score showing how well the vector matches the query, using a cover density algorithm. See Section 12.3.3 for details.
            "ts_rewrite", // Replaces occurrences of target with substitute within the query. See Section
            "tsquery_phrase", // Constructs a phrase query that searches for matches of query1 and query2 at successive lexemes (same as <-> operator).
            "tsvector_to_array", // Converts a tsvector to an array of lexemes.

            // Text search debugging functions see https://www.postgresql.org/docs/14/functions-textsearch.html#TEXTSEARCH-FUNCTIONS-DEBUG-TABLE
            "ts_debug", // Extracts and normalizes tokens from the document according to the specified or default text search configuration, and returns information about how each token was processed. See Section 12.8.1 for details.
            "ts_lexize", // Returns an array of replacement lexemes if the input token is known to the dictionary, or an empty array if the token is known to the dictionary but it is a stop word, or NULL if it is not a known word. See Section 12.8.3 for details.
            "ts_parse", // Extracts tokens from the document using the named parser. See Section 12.8.2 for details.
            "ts_token_type", // Returns a table that describes each type of token the named parser can recognize. See Section 12.8.2 for details.

            // UUID Functions see https://www.postgresql.org/docs/14/functions-uuid.html
            "gen_random_uuid", // Generate a version 4 (random) UUID

            // XML Functions see https://www.postgresql.org/docs/14/functions-xml.html
            "xmlcomment", // Creates an XML comment
            "xmlconcat", // Concatenates XML values
            "xmlelement", // Creates an XML element
            "xmlforest", // Creates an XML forest (sequence) of elements
            "xmlpi", // Creates an XML processing instruction
            "xmlagg", // Concatenates the input values to the aggregate function call, much like xmlconcat does, except that concatenation occurs across rows rather than across expressions in a single row.
            "xmlexists", // Evaluates an XPath 1.0 expression (the first argument), with the passed XML value as its context item.
            "xml_is_well_formed", // Checks whether the argument is a well-formed XML document or fragment.
            "xml_is_well_formed_content", // Checks whether the argument is a well-formed XML document or fragment, and that it contains no document type declaration.
            "xml_is_well_formed_document", // Checks whether the argument is a well-formed XML document.
            "xpath", // Evaluates the XPath 1.0 expression xpath (given as text) against the XML value xml.
            "xpath_exists", // Evaluates the XPath 1.0 expression xpath (given as text) against the XML value xml, and returns true if the expression selects at least one node, otherwise false.
            "xmltable", // Expands an XML value into a table whose columns match the rowtype defined by the function's parameter list.
            "table_to_xml", // Converts a table to XML.
            "cursor_to_xml", // Converts a cursor to XML.

                // JSON and JSONB creation functions see https://www.postgresql.org/docs/14/functions-json.html#FUNCTIONS-JSON-CREATION-TABLE
            "to_json", // Converts any SQL value to JSON.
            "to_jsonb", // Converts any SQL value to JSONB.
            "array_to_json", // Converts an SQL array to a JSON array.
            "row_to_json", // Converts an SQL composite value to a JSON object.
            "json_build_array", // Builds a possibly-heterogeneously-typed JSON array out of a variadic argument list.
            "jsonb_build_array", // Builds a possibly-heterogeneously-typed JSON array out of a variadic argument list.
            "json_build_object", // Builds a JSON object out of a variadic argument list.
            "json_object", // Builds a JSON object out of a text array.
            "jsonb_object", // Builds a JSONB object out of a text array.

            // JSON and JSONB processing functions see https://www.postgresql.org/docs/14/functions-json.html#FUNCTIONS-JSON-PROCESSING-TABLE
            "json_array_elements", // Expands the top-level JSON array into a set of JSON values.
            "jsonb_array_elements", // Expands the top-level JSON array into a set of JSONB values.
            "json_array_elements_text", // Expands the top-level JSON array into a set of text values.
            "jsonb_array_elements_text", // Expands the top-level JSONB array into a set of text values.
            "json_array_length", // Returns the number of elements in the top-level JSON array.
            "jsonb_array_length", // Returns the number of elements in the top-level JSONB array.
            "json_each", // Expands the top-level JSON object into a set of key/value pairs.
            "jsonb_each", // Expands the top-level JSONB object into a set of key/value pairs.
            "json_each_text", // Expands the top-level JSON object into a set of key/value pairs. The returned values will be of type text.
            "jsonb_each_text", // Expands the top-level JSONB object into a set of key/value pairs. The returned values will be of type text.
            "json_extract_path", // Extracts JSON sub-object at the specified path.
            "jsonb_extract_path", // Extracts JSONB sub-object at the specified path.
            "json_extract_path_text", // Extracts JSON sub-object at the specified path as text.
            "jsonb_extract_path_text", // Extracts JSONB sub-object at the specified path as text.
            "json_object_keys", // Returns the set of keys in the top-level JSON object.
            "jsonb_object_keys", // Returns the set of keys in the top-level JSONB object.
            "json_populate_record", // Expands the top-level JSON object to a row having the composite type of the base argument.
            "jsonb_populate_record", // Expands the top-level JSON object to a row having the composite type of the base argument.
            "json_populate_recordset", // Expands the top-level JSON array of objects to a set of rows having the composite type of the base argument.
            "jsonb_populate_recordset", // Expands the top-level JSONB array of objects to a set of rows having the composite type of the base argument.
            "json_to_record", // Expands the top-level JSON object to a row having the composite type defined by an AS clause.
            "jsonb_to_record", // Expands the top-level JSONB object to a row having the composite type defined by an AS clause.
            "json_to_recordset", // Expands the top-level JSON array of objects to a set of rows having the composite type defined by an AS clause.
            "jsonb_to_recordset", // Expands the top-level JSONB array of objects to a set of rows having the composite type defined by an AS clause.
            "json_strip_nulls", // Deletes all object fields that have null values from the given JSON value, recursively.
            "jsonb_strip_nulls", // Deletes all object fields that have null values from the given JSONB value, recursively.
            "jsonb_set", // Returns target with the item designated by path replaced by new_value, or with new_value added if create_if_missing is true (which is the default) and the item designated by path does not exist.
            "jsonb_set_lax", // If new_value is not NULL, behaves identically to jsonb_set. Otherwise behaves according to the value of null_value_treatment which must be one of 'raise_exception', 'use_json_null', 'delete_key', or 'return_target'. The default is 'use_json_null'.
            "jsonb_insert", //Returns target with new_value inserted.
            "jsonb_path_exists", // Checks whether the JSON path returns any item for the specified JSON value.
            "jsonb_path_match", // Returns the result of a JSON path predicate check for the specified JSON value.
            "jsonb_path_query", // Returns all JSON items returned by the JSON path for the specified JSON value.
            "jsonb_path_query_array", // Returns all JSON items returned by the JSON path for the specified JSON value, as a JSON array.
            "jsonb_path_query_first", // Returns the first JSON item returned by the JSON path for the specified JSON value. Returns NULL if there are no results.
            "jsonb_path_exists_tz", // Support comparisons of date/time values that require timezone-aware conversions.
            "jsonb_path_match_tz", // Support comparisons of date/time values that require timezone-aware conversions.
            "jsonb_path_query_tz", // Support comparisons of date/time values that require timezone-aware conversions.
            "jsonb_path_query_array_tz", // Support comparisons of date/time values that require timezone-aware conversions.
            "jsonb_path_query_first_tz", // Support comparisons of date/time values that require timezone-aware conversions.
            "jsonb_pretty", // Converts the given JSON value to pretty-printed, indented text.
            "json_typeof", // Returns the type of the top-level JSON value as a text string.
            "jsonb_typeof", // Returns the type of the top-level JSONB value as a text string.

            // Conditional Expressions hhttps://www.postgresql.org/docs/14/functions-conditional.html
            "coalesce", // Return first non-null argument.
            "nullif", // Return null if two arguments are equal, otherwise return the first argument.
            "greatest", // Return greatest of a list of values.
            "least", // Return smallest of a list of values.

            // Array Functions https://www.postgresql.org/docs/14/functions-array.html#ARRAY-FUNCTIONS-TABLE
            "array_append", // Appends an element to the end of an array (same as the || operator).
            "array_cat", // Concatenates two arrays (same as the || operator).
            "array_dims", // Returns a text representation of the array's dimensions.
            "array_fill", // Returns an array filled with copies of the given value, having dimensions of the lengths specified by the second argument. The optional third argument supplies lower-bound values for each dimension (which default to all 1).
            "array_length", // Returns the length of the requested array dimension. (Produces NULL instead of 0 for empty or missing array dimensions.)
            "array_lower", // Returns the lower bound of the requested array dimension.
            "array_ndims", // Returns the number of dimensions of the array.
            "array_position", // Returns the subscript of the first occurrence of the second argument in the array, or NULL if it's not present.
            "array_prepend", // Prepends an element to the beginning of an array (same as the || operator).
            "array_remove", // Removes all elements equal to the given value from the array. The array must be one-dimensional. Comparisons are done using IS NOT DISTINCT FROM semantics, so it is possible to remove NULLs.
            "array_replace", // Replaces each array element equal to the second argument with the third argument.
            "array_to_string", // Converts each array element to its text representation, and concatenates those separated by the delimiter string. If null_string is given and is not NULL, then NULL array entries are represented by that string; otherwise, they are omitted.
            "array_upper", // Returns the upper bound of the requested array dimension.
            "cardinality", // Returns the total number of elements in the array, or 0 if the array is empty.
            "trim_array", // Trims an array by removing the last n elements. If the array is multidimensional, only the first dimension is trimmed.
            "unnest", // Expands an array into a set of rows. The array's elements are read out in storage order.

            // Range Functions https://www.postgresql.org/docs/14/functions-range.html#RANGE-FUNCTIONS-TABLE
            "lower", // Extracts the lower bound of the range (NULL if the range is empty or the lower bound is infinite).
            "upper", // Extracts the upper bound of the range (NULL if the range is empty or the upper bound is infinite).
            "isempty", // Is the range empty?
            "lower_inc", // Is the range's lower bound inclusive?
            "upper_inc", // Is the range's upper bound inclusive?
            "lower_inf", // Is the range's lower bound infinite?
            "upper_inf", // Is the range's upper bound infinite?
            "range_merge", // Computes the smallest range that includes both of the given ranges.

            // Multi-range Functions https://www.postgresql.org/docs/14/functions-range.html#MULTIRANGE-FUNCTIONS-TABLE
            "multirange", // Returns a multirange containing just the given range.

            // General purpose aggregate functions https://www.postgresql.org/docs/14/functions-aggregate.html#FUNCTIONS-AGGREGATE-TABLE
            "array_agg", // Collects all the input values, including nulls, into an array.
            "avg", // Computes the average (arithmetic mean) of all the non-null input values.
            "bit_and", // Computes the bitwise AND of all non-null input values.
            "bit_or", // Computes the bitwise OR of all non-null input values.
            "bit_xor", // Computes the bitwise exclusive OR of all non-null input values. Can be useful as a checksum for an unordered set of values.
            "bool_and", // Returns true if all non-null input values are true, otherwise false.
            "bool_or", // Returns true if any non-null input value is true, otherwise false.
            "count", // Computes the number of input rows.
            "every", // This is the SQL standard's equivalent to bool_and.
            "json_agg", // Collects all the input values, including nulls, into a JSON array. Values are converted to JSON as per to_json or to_jsonb.
            "json_object_agg", // Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per to_json or to_jsonb. Values can be null, but not keys.
            "max", // Computes the maximum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as inet, interval, money, oid, pg_lsn, tid, and arrays of any of these types.
            "min", // Computes the minimum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as inet, interval, money, oid, pg_lsn, tid, and arrays of any of these types.
            "range_agg", // Computes the union of the non-null input values.
            "range_intersect_agg", // Computes the intersection of the non-null input values.
            "string_agg", // Concatenates the non-null input values into a string. Each value after the first is preceded by the corresponding delimiter (if it's not null).
            "sum", // Computes the sum of the non-null input values.
            "xmlagg", // Concatenates the non-null XML input values.

            // Statistical aggregate functions https://www.postgresql.org/docs/14/functions-aggregate.html#FUNCTIONS-AGGREGATE-STATISTICS-TABLE
            "corr", // Computes the correlation coefficient.
            "covar_pop", // Computes the population covariance.
            "covar_samp", // Computes the sample covariance.
            "regr_avgx", // Computes the average of the independent variable, sum(X)/N.
            "regr_avgy", // Computes the average of the dependent variable, sum(Y)/N.
            "regr_count", // Computes the number of rows in which both inputs are non-null.
            "regr_intercept", // Computes the y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs.
            "regr_r2", // Computes the square of the correlation coefficient.
            "regr_slope", // Computes the slope of the least-squares-fit linear equation determined by the (X, Y) pairs.
            "regr_sxx", // Computes the “sum of squares” of the independent variable, sum(X^2) - sum(X)^2/N.
            "regr_sxy", // Computes the “sum of products” of independent times dependent variables, sum(X*Y) - sum(X) * sum(Y)/N.
            "regr_syy", // Computes the “sum of squares” of the dependent variable, sum(Y^2) - sum(Y)^2/N.
            "stddev", // This is a historical alias for stddev_samp.
            "stddev_pop", // Computes the population standard deviation of the input values.
            "stddev_samp", // Computes the sample standard deviation of the input values.
            "variance", // This is a historical alias for var_samp.
            "var_pop", // Computes the population variance of the input values (square of the population standard deviation).
            "var_samp", // Computes the sample variance of the input values (square of the sample standard deviation).

            // Ordered-set aggregate functions https://www.postgresql.org/docs/14/functions-aggregate.html#FUNCTIONS-AGGREGATE-ORDEREDSET-TABLE
            "mode", // Computes the mode (most frequent value) of the input values.
            "percentile_cont", // Computes the continuous percentile of the input values.
            "percentile_disc", // Computes the discrete percentile of the input values.

            // Hypothetical-set aggregate functions https://www.postgresql.org/docs/14/functions-aggregate.html#FUNCTIONS-AGGREGATE-HYPOTHETICAL-TABLE
            "rank", // Computes the rank of the current row with gaps; same as row_number of its first peer.
            "dense_rank", // Computes the rank of the current row without gaps; this function counts peer groups.
            "percent_rank", // Computes the relative rank (percentile) of the current row: (rank - 1) / (total partition rows - 1).
            "cume_dist", // Computes the relative rank of the current row: (number of partition rows preceding or peer with current row) / (total partition rows).

            // Grouping set aggregate functions https://www.postgresql.org/docs/14/functions-aggregate.html#FUNCTIONS-AGGREGATE-GROUPINGSET-TABLE
            "grouping", // Returns a bit mask indicating which GROUP BY expressions are not included in the current grouping set.

            // Window functions https://www.postgresql.org/docs/14/functions-window.html#FUNCTIONS-WINDOW-TABLE
            "row_number", // Number of the current row within its partition, counting from 1.
            "ntile", // Integer ranging from 1 to the argument value, dividing the partition as equally as possible.
            "lag", // Returns value evaluated at the row that is offset rows before the current row within the partition; if there is no such row, instead returns default (which must be of a type compatible with value).
            "lead", // Returns value evaluated at the row that is offset rows after the current row within the partition; if there is no such row, instead returns default (which must be of a type compatible with value).
            "first_value", // Returns value evaluated at the row that is the first row of the window frame.
            "last_value", // Returns value evaluated at the row that is the last row of the window frame.
            "nth_value", // Returns value evaluated at the row that is the n'th row of the window frame (counting from 1); returns NULL if there is no such row.

            // Set returning functions https://www.postgresql.org/docs/14/functions-srf.html
            "generate_series", // Expands range arguments into a set of rows.
            "generate_subscripts", // Expands array arguments into a set of rows.

            // Abbreivated syntax for common functions
            "pow", // see power function
            "date", // see to_date

       ].into_iter().collect()
    };
}

pub(super) static SQL_DIALECT: PostgreSqlDialect = PostgreSqlDialect {};
