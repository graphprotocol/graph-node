use std::collections::HashSet;

use lazy_static::lazy_static;
use sqlparser::dialect::PostgreSqlDialect;

lazy_static! {
    pub(super) static ref POSTGRES_WHITELISTED_FUNCTIONS: HashSet<&'static str> = {
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

lazy_static! {
    pub(super) static ref POSTGRES_BLACKLISTED_FUNCTIONS: HashSet<&'static str> = {
        vec![
            "query_to_xml", // Converts a query result to XML.

            "ts_stat", // Executes the sqlquery, which must return a single tsvector column, and returns statistics about each distinct lexeme contained in the data. See Section 12.4.4 for details.

            "pg_client_encoding", // Returns current client encoding name

            // Delay execution see https://www.postgresql.org/docs/14/functions-datetime.html
            "pg_sleep", // Delay for the specified number of seconds
            "pg_sleep_for", // Delay for the specified amount of time
            "pg_sleep_until", // Delay until the specified time

            // Session Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
            "current_catalog", // Returns the name of the current database.
            "current_database", // Returns the name of the current database.
            "current_query", // Returns the text of the currently executing query, as submitted by the client (which might contain more than one statement).
            "current_role", // Returns the user name of the current execution context.
            "current_schema", // Returns the name of the schema that is first in the search path (or a null value if the search path is empty). This is the schema that will be used for any tables or other named objects that are created without specifying a target schema.
            "current_schemas", // Returns an array of the names of all schemas presently in the effective search path, in their priority order.
            "current_user", // Returns the user name of the current execution context.
            "inet_client_addr", // Returns the IP address of the current client, or NULL if the current connection is via a Unix-domain socket.
            "inet_client_port", // Returns the IP port number of the current client, or NULL if the current connection is via a Unix-domain socket.
            "inet_server_addr", // Returns the IP address on which the server accepted the current connection, or NULL if the current connection is via a Unix-domain socket.
            "inet_server_port", // Returns the IP port number on which the server accepted the current connection, or NULL if the current connection is via a Unix-domain socket.
            "pg_backend_pid", // Returns the process ID of the server process attached to the current session.
            "pg_blocking_pids", // Returns an array of the process ID(s) of the sessions that are blocking the server process with the specified process ID from acquiring a lock, or an empty array if there is no such server process or it is not blocked.
            "pg_conf_load_time", // Returns the time when the server configuration files were last loaded.
            "pg_current_logfile", // Returns the path name of the log file currently in use by the logging collector.
            "pg_is_in_recovery", // Returns true if recovery is still in progress in the current server process, false otherwise.
            "pg_last_copy_count", // Returns the number of rows copied (using COPY) by the last command executed in the current session.
            "pg_last_copy_id", // Returns the ID of the last COPY command executed in the current session.
            "pg_last_query_id", // Returns the ID of the last query executed in the current session.
            "pg_last_query_sample", // Returns the query sample of the last query executed in the current session.
            "pg_last_xact_replay_timestamp", // Returns the timestamp of the last transaction commit/rollback applied in the current session.
            "pg_last_xact_replay_timestamp_origin", // Returns the origin of the last transaction commit/rollback applied in the current session.
            "pg_listening_channels", // Returns the set of names of asynchronous notification channels that the current session is listening to.
            "pg_notification_queue_usage", // Returns the fraction (0–1) of the asynchronous notification queue's maximum size that is currently occupied by notifications that are waiting to be processed.
            "pg_postmaster_start_time", // Returns the time when the server started.
            "pg_safe_snapshot_blocking_pids", // Returns an array of the process ID(s) of the sessions that are blocking the server process with the specified process ID from acquiring a safe snapshot, or an empty array if there is no such server process or it is not blocked.
            "pg_trigger_depth", // Returns the current nesting level of PostgreSQL triggers (0 if not called, directly or indirectly, from inside a trigger).
            "session_user", // Returns the session user's name.
            "user", // This is equivalent to current_user.
            "version", // Returns a string describing the PostgreSQL server's version. You can also get this information from server_version, or for a machine-readable version use server_version_num. Software developers should use server_version_num (available since 8.2) or PQserverVersion instead of parsing the text version.

            // Access Privilege Inquiry Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-ACCESS-TABLE
            "has_any_column_privilege", // Does user have privilege for any column of table?
            "has_column_privilege", // Does user have privilege for the specified table column?
            "has_database_privilege", // Does user have privilege for database?
            "has_foreign_data_wrapper_privilege", // Does user have privilege for foreign-data wrapper?
            "has_function_privilege", // Does user have privilege for function?
            "has_language_privilege", // Does user have privilege for language?
            "has_schema_privilege", // Does user have privilege for schema?
            "has_sequence_privilege", // Does user have privilege for sequence?
            "has_server_privilege", // Does user have privilege for foreign server?
            "has_table_privilege", // Does user have privilege for table?
            "has_tablespace_privilege", // Does user have privilege for tablespace?
            "has_type_privilege", // Does user have privilege for data type?
            "pg_has_role", // Does user have privilege for role?
            "row_security_active", // Is row-level security active for the specified table in the context of the current user and current environment?

            // ACL item functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-ACLITEM-FN-TABLE
            "acldefault", // Constructs an aclitem array holding the default access privileges for an object of type type belonging to the role with OID ownerId.
            "aclexplode", // Returns the aclitem array as a set of rows.
            "makeaclitem", // Constructs an aclitem with the given properties.

            // Schema Visibility Inquiry Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-SCHEMA-TABLE
            "pg_collation_is_visible", // Is collation visible in search path?
            "pg_conversion_is_visible", // Is conversion visible in search path?
            "pg_function_is_visible", // Is function visible in search path? (This also works for procedures and aggregates.)
            "pg_opclass_is_visible", // Is operator class visible in search path?
            "pg_operator_is_visible", // Is operator visible in search path?
            "pg_opfamily_is_visible", // Is operator family visible in search path?
            "pg_statistics_obj_is_visible", // Is statistics object visible in search path?
            "pg_table_is_visible", // Is table visible in search path? (This works for all types of relations, including views, materialized views, indexes, sequences and foreign tables.)
            "pg_ts_config_is_visible", // Is text search configuration visible in search path?
            "pg_ts_dict_is_visible", // Is text search dictionary visible in search path?
            "pg_ts_parser_is_visible", // Is text search parser visible in search path?
            "pg_ts_template_is_visible", // Is text search template visible in search path?
            "pg_type_is_visible", // Is type (or domain) visible in search path?

            // System Catalog Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
            "format_type", // Returns the SQL name of a data type that is identified by its type OID and possibly a type modifier.
            "pg_get_catalog_foreign_keys", // Returns a set of records describing the foreign key relationships that exist within the PostgreSQL system catalogs.
            "pg_get_constraintdef", // Returns the definition of a constraint.
            "pg_get_expr", // Returns the definition of an expression.
            "pg_get_functiondef", // Returns the definition of a function or procedure.
            "pg_get_function_arguments", // Returns the argument list of a function or procedure.
            "pg_get_function_identity_arguments", // Returns the argument list necessary to identify a function or procedure.
            "pg_get_function_result", // Returns the return type of a function or procedure.
            "pg_get_indexdef", // Returns the definition of an index.
            "pg_get_keywords", // Returns a set of records describing the SQL keywords recognized by the server.
            "pg_get_ruledef", // Returns the definition of a rule.
            "pg_get_serial_sequence", // Returns the name of the sequence associated with a column, or NULL if no sequence is associated with the column.
            "pg_get_statisticsobjdef", // Returns the definition of an extended statistics object.
            "pg_get_triggerdef", // Returns the definition of a trigger.
            "pg_get_userbyid", // Returns a role's name given its OID.
            "pg_get_viewdef", // Returns the definition of a view.
            "pg_index_column_has_property", // Tests whether an index column has the named property.
            "pg_index_has_property", // Tests whether an index has the named property.
            "pg_indexam_has_property", // Tests whether an index access method has the named property.
            "pg_options_to_table", // Returns the set of storage options represented by a value from pg_class.reloptions or pg_attribute.attoptions.
            "pg_tablespace_databases", // Returns the set of OIDs of databases that have objects stored in the specified tablespace.
            "pg_tablespace_location", // Returns the file system path that this tablespace is located in.
            "pg_typeof", // Returns the OID of the data type of the value that is passed to it.
            "to_regclass", // Translates a textual relation name to its OID.
            "to_regcollation", // Translates a textual collation name to its OID.
            "to_regnamespace", // Translates a textual schema name to its OID.
            "to_regoper", // Translates a textual operator name to its OID.
            "to_regoperator", // Translates a textual operator name (with parameter types) to its OID.
            "to_regproc", // Translates a textual function or procedure name to its OID.
            "to_regprocedure", // Translates a textual function or procedure name (with argument types) to its OID.
            "to_regrole", // Translates a textual role name to its OID.
            "to_regtype", // Translates a textual type name to its OID.

            // Comment Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-COMMENT-TABLE
            "col_description", // Returns the comment for a table column, which is specified by the OID of its table and its column number.
            "obj_description", // Returns the comment for a database object specified by its OID and the name of the containing system catalog.
            "shobj_description", // Returns the comment for a shared database object specified by its OID and the name of the containing system catalog.

            // Transaction ID and Snapshot Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-INFO-TXID-SNAPSHOT-TABLE
            "pg_current_xact_id", // Returns the current transaction's ID.
            "pg_current_xact_id_if_assigned", // Returns the current transaction's ID, or NULL if no ID is assigned yet.
            "pg_xact_status", // Reports the commit status of a recent transaction.
            "pg_current_snapshot", // Returns a current snapshot, a data structure showing which transaction IDs are now in-progress.
            "pg_snapshot_xip", // Returns the set of in-progress transaction IDs contained in a snapshot.
            "pg_snapshot_xmax", // Returns the xmax of a snapshot.
            "pg_snapshot_xmin", // Returns the xmin of a snapshot.
            "pg_visible_in_snapshot", // Is the given transaction ID visible according to this snapshot (that is, was it completed before the snapshot was taken)? Note that this function will not give the correct answer for a subtransaction ID.

            // Deprecated Transaction ID and Snapshot Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-TXID-SNAPSHOT
            "txid_current", // Returns the current transaction's ID.
            "txid_current_if_assigned", // Returns the current transaction's ID, or NULL if no ID is assigned yet.
            "txid_current_snapshot", // Returns a current snapshot, a data structure showing which transaction IDs are now in-progress.
            "txid_snapshot_xip", // Returns the set of in-progress transaction IDs contained in a snapshot.
            "txid_snapshot_xmax", // Returns the xmax of a snapshot.
            "txid_snapshot_xmin", // Returns the xmin of a snapshot.
            "txid_visible_in_snapshot", // Is the given transaction ID visible according to this snapshot (that is, was it completed before the snapshot was taken)? Note that this function will not give the correct answer for a subtransaction ID.
            "txid_status", // Reports the commit status of a recent transaction.

            // Committed Transaction Information Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-COMMIT-TIMESTAMP
            "pg_xact_commit_timestamp", // Returns the commit timestamp of a transaction.
            "pg_xact_commit_timestamp_origin", // Returns the commit timestamp and replication origin of a transaction.
            "pg_last_committed_xact", // Returns the transaction ID, commit timestamp and replication origin of the latest committed transaction.

            // Control Data Functions https://www.postgresql.org/docs/14/functions-info.html#FUNCTIONS-CONTROLDATA
            "pg_control_checkpoint", // Returns information about current checkpoint state.
            "pg_control_init", // Returns information about cluster initialization state.
            "pg_control_recovery", // Returns information about recovery state.
            "pg_control_system", // Returns information about current control file state.

            // Configuration Settings Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-SET-TABLE
            "current_setting", // Returns the current value of the parameter setting_name.
            "set_config", // Sets the parameter setting_name to new_value, and returns that value.

            // Server Signaling Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
            "pg_cancel_backend", // Cancels the current query of the session whose backend process has the specified process ID.
            "pg_log_backend_memory_contexts", // Requests to log the memory contexts of the backend with the specified process ID.
            "pg_reload_conf", // Causes all processes of the PostgreSQL server to reload their configuration files.
            "pg_rotate_logfile", // Signals the log-file manager to switch to a new output file immediately.
            "pg_terminate_backend", // Terminates the session whose backend process has the specified process ID.

            // Backup Control Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-BACKUP-TABLE
            "pg_create_restore_point", // Creates a named marker record in the write-ahead log that can later be used as a recovery target, and returns the corresponding write-ahead log location.
            "pg_current_wal_flush_lsn", // Returns the current write-ahead log flush location (see notes below).
            "pg_current_wal_insert_lsn", // Returns the current write-ahead log insert location (see notes below).
            "pg_current_wal_lsn", // Returns the current write-ahead log write location (see notes below).
            "pg_start_backup", // Prepares the server to begin an on-line backup.
            "pg_stop_backup", // Finishes performing an exclusive or non-exclusive on-line backup.
            "pg_is_in_backup", // Returns true if an on-line exclusive backup is in progress.
            "pg_backup_start_time", // Returns the start time of the current on-line exclusive backup if one is in progress, otherwise NULL.
            "pg_switch_wal", // Forces the server to switch to a new write-ahead log file, which allows the current file to be archived (assuming you are using continuous archiving).
            "pg_walfile_name_offset", // Returns the file name and byte offset of the current write-ahead log file.
            "pg_walfile_name", // Returns the file name of the current write-ahead log file.
            "pg_wal_lsn_diff", // Calculates the difference in bytes (lsn1 - lsn2) between two write-ahead log locations.

            // Recovery Information Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE
            "pg_is_in_recovery", // Returns true if recovery is still in progress.
            "pg_last_wal_receive_lsn", // Returns the last write-ahead log location that has been received and synced to disk by streaming replication.
            "pg_last_wal_replay_lsn", // Returns the last write-ahead log location that has been replayed during recovery.
            "pg_last_xact_replay_timestamp", // Returns the time stamp of the last transaction replayed during recovery.

            // Recovery Control Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL-TABLE
            "pg_is_wal_replay_paused", // Returns true if recovery pause is requested.
            "pg_get_wal_replay_pause_state", // Returns recovery pause state.
            "pg_promote", // Promotes a standby server to primary status.
            "pg_wal_replay_pause", // Request to pause recovery.
            "pg_wal_replay_resume", // Restarts recovery if it was paused.

            // Snapshot Synchronization Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE
            "pg_export_snapshot", // Saves the transaction's current snapshot and returns a text string identifying the snapshot.

            // Replication Management Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-REPLICATION-TABLE
            "pg_create_physical_replication_slot", // Creates a new physical replication slot.
            "pg_create_logical_replication_slot", // Creates a new logical replication slot.
            "pg_drop_replication_slot", // Drops the named replication slot.
            "pg_copy_physical_replication_slot", // Copies an existing physical replication slot named src_slot_name to a physical replication slot.
            "pg_copy_logical_replication_slot", // Copies an existing logical replication slot named src_slot_name to a logical replication slot.
            "pg_logical_slot_get_changes", // Returns changes in the slot slot_name, starting from the point from which changes have been consumed last.
            "pg_logical_slot_peek_changes", // Behaves just like the pg_logical_slot_get_changes() function, except that changes are not consumed; that is, they will be returned again on future calls.
            "pg_logical_slot_get_binary_changes", // Behaves just like the pg_logical_slot_get_changes() function, except that changes are returned as bytea.
            "pg_logical_slot_peek_binary_changes", // Behaves just like the pg_logical_slot_peek_changes() function, except that changes are returned as bytea.
            "pg_replication_slot_advance", // Advances the current confirmed position of a replication slot named slot_name.
            "pg_replication_origin_create", // Creates a replication origin with the given external name, and returns the internal ID assigned to it.
            "pg_replication_origin_drop", // Deletes a previously-created replication origin, including any associated replay progress.
            "pg_replication_origin_oid", // Looks up the internal ID of a previously-created replication origin.
            "pg_replication_origin_session_setup", // Marks the current session as replaying from the given origin, allowing replay progress to be tracked.
            "pg_replication_origin_session_reset", // Cancels the effects of pg_replication_origin_session_setup().
            "pg_replication_origin_session_is_setup", // Returns true if a replication origin has been selected in the current session.
            "pg_replication_origin_session_progress", // Returns the replay location for the replication origin selected in the current session.
            "pg_replication_origin_xact_setup", // Marks the current transaction as replaying a transaction that has committed at the given LSN and timestamp.
            "pg_replication_origin_xact_reset", // Cancels the effects of pg_replication_origin_xact_setup().
            "pg_replication_origin_advance", // Sets replication progress for the given node to the given location.
            "pg_replication_origin_progress", // Returns the replay location for the given replication origin.
            "pg_logical_emit_message", // Emits a logical decoding message.

            // Database Object Size Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
            "pg_column_size", // Shows the number of bytes used to store any individual data value.
            "pg_column_compression", // Shows the compression algorithm that was used to compress an individual variable-length value.
            "pg_database_size", // Computes the total disk space used by the database with the specified name or OID.
            "pg_indexes_size", // Computes the total disk space used by indexes attached to the specified table.
            "pg_relation_size", // Computes the disk space used by one “fork” of the specified relation.
            "pg_size_bytes", // Converts a size in human-readable format into bytes.
            "pg_size_pretty", // Converts a size in bytes into a more easily human-readable format with size units (bytes, kB, MB, GB or TB as appropriate).
            "pg_table_size", // Computes the disk space used by the specified table, excluding indexes (but including its TOAST table if any, free space map, and visibility map).
            "pg_tablespace_size", // Computes the total disk space used in the tablespace with the specified name or OID.
            "pg_total_relation_size", // Computes the total disk space used by the specified table, including all indexes and TOAST data.

            // Database Object Location Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-DBLOCATION
            "pg_relation_filenode", // Returns the “filenode” number currently assigned to the specified relation.
            "pg_relation_filepath", // Returns the entire file path name of the relation.
            "pg_filenode_relation", // Returns a relation's OID given the tablespace OID and filenode it is stored under.

            // Collation Management Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-COLLATION
            "pg_collation_actual_version", // Returns the actual version of the collation object as it is currently installed in the operating system.
            "pg_import_system_collations", // Adds collations to the system catalog pg_collation based on all the locales it finds in the operating system.

            // Partitioning Information Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-INFO-PARTITION
            "pg_partition_tree", // Lists the tables or indexes in the partition tree of the given partitioned table or partitioned index, with one row for each partition.
            "pg_partition_ancestors", // Lists the ancestor relations of the given partition, including the relation itself.
            "pg_partition_root", // Returns the top-most parent of the partition tree to which the given relation belongs.

            // Index Maintenance Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-INDEX-TABLE
            "brin_summarize_new_values", // Scans the specified BRIN index to find page ranges in the base table that are not currently summarized by the index; for any such range it creates a new summary index tuple by scanning those table pages.
            "brin_summarize_range", // Summarizes the page range covering the given block, if not already summarized.
            "brin_desummarize_range", // Removes the BRIN index tuple that summarizes the page range covering the given table block, if there is one.
            "gin_clean_pending_list", // Cleans up the “pending” list of the specified GIN index by moving entries in it, in bulk, to the main GIN data structure.

            // Generic File Access Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-GENFILE-TABLE
            "pg_ls_dir", // Returns the names of all files (and directories and other special files) in the specified directory.
            "pg_ls_logdir", // Returns the name, size, and last modification time (mtime) of each ordinary file in the server's log directory.
            "pg_ls_waldir", // Returns the name, size, and last modification time (mtime) of each ordinary file in the server's write-ahead log (WAL) directory.
            "pg_ls_archive_statusdir", // Returns the name, size, and last modification time (mtime) of each ordinary file in the server's WAL archive status directory (pg_wal/archive_status).
            "pg_ls_tmpdir", // Returns the name, size, and last modification time (mtime) of each ordinary file in the temporary file directory for the specified tablespace.
            "pg_read_file", // Returns all or part of a text file, starting at the given byte offset, returning at most length bytes (less if the end of file is reached first).
            "pg_read_binary_file", // Returns all or part of a file. This function is identical to pg_read_file except that it can read arbitrary binary data, returning the result as bytea not text; accordingly, no encoding checks are performed.

            // Advisory Lock Functions https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS-TABLE
            "pg_advisory_lock", // Obtains an exclusive session-level advisory lock, waiting if necessary.
            "pg_advisory_lock_shared", // Obtains a shared session-level advisory lock, waiting if necessary.
            "pg_advisory_unlock", // Releases a previously-acquired exclusive session-level advisory lock.
            "pg_advisory_unlock_all", // Releases all session-level advisory locks held by the current session.
            "pg_advisory_unlock_shared", // Releases a previously-acquired shared session-level advisory lock.
            "pg_advisory_xact_lock", // Obtains an exclusive transaction-level advisory lock, waiting if necessary.
            "pg_advisory_xact_lock_shared", // Obtains a shared transaction-level advisory lock, waiting if necessary.
            "pg_try_advisory_lock", // Obtains an exclusive session-level advisory lock if available.
            "pg_try_advisory_lock_shared", // Obtains a shared session-level advisory lock if available.
            "pg_try_advisory_xact_lock", // Obtains an exclusive transaction-level advisory lock if available.
            "pg_try_advisory_xact_lock_shared", // Obtains a shared transaction-level advisory lock if available.

            // Built-In Trigger Functions https://www.postgresql.org/docs/14/functions-trigger.html#BUILTIN-TRIGGERS-TABLE
            "suppress_redundant_updates_trigger", // uppresses do-nothing update operations.
            "tsvector_update_trigger", // Automatically updates a tsvector column from associated plain-text document column(s).
            "tsvector_update_trigger_column", // Automatically updates a tsvector column from associated plain-text document column(s).

            // Event Trigger Functions https://www.postgresql.org/docs/14/functions-event-triggers.html
            "pg_event_trigger_dropped_objects", // Returns the list of objects dropped by the current command.
            "pg_event_trigger_ddl_commands", // Returns the list of DDL commands executed by the current command.
            "pg_event_trigger_table_rewrite_oid", // Returns the OID of the table that is being rewritten by the current command.
            "pg_event_trigger_table_rewrite_reason", // Returns the reason why the current command is rewriting a table.

            // Sequence manipulation functions https://www.postgresql.org/docs/14/functions-sequence.html#FUNCTIONS-SEQUENCE-TABLE
            "nextval", // Advance sequence and return new value
            "setval", // Set sequence's current value
            "currval", // Return value most recently obtained with nextval
            "lastval", // Return value most recently obtained with nextval

        ].into_iter().collect()
    };
}

pub(super) static SQL_DIALECT: PostgreSqlDialect = PostgreSqlDialect {};
