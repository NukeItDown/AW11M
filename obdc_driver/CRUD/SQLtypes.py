import datetime, ctypes, sys
from decimal import Decimal

create_buffer = ctypes.create_string_buffer
create_buffer_u = ctypes.create_unicode_buffer

SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC2, SQL_OV_ODBC3 = 200, 2, 3
SQL_DRIVER_NOPROMPT = 0
SQL_ATTR_CONNECTION_POOLING = 201; SQL_CP_ONE_PER_HENV = 2

SQL_FETCH_NEXT, SQL_FETCH_FIRST, SQL_FETCH_LAST = 0x01, 0x02, 0x04
SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3
SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_ERROR = 0, 1, -1
SQL_NO_DATA = 100; SQL_NO_TOTAL = -4
SQL_ATTR_ACCESS_MODE = SQL_ACCESS_MODE = 101
SQL_ATTR_AUTOCOMMIT = SQL_AUTOCOMMIT = 102

SQL_MODE_DEFAULT = SQL_MODE_READ_WRITE = 0; SQL_MODE_READ_ONLY = 1
SQL_AUTOCOMMIT_OFF, SQL_AUTOCOMMIT_ON = 0, 1
SQL_IS_UINTEGER = -5
SQL_ATTR_LOGIN_TIMEOUT = 103; SQL_ATTR_CONNECTION_TIMEOUT = 113;SQL_ATTR_QUERY_TIMEOUT = 0
SQL_COMMIT, SQL_ROLLBACK = 0, 1

SQL_INDEX_UNIQUE,SQL_INDEX_ALL = 0,1
SQL_QUICK,SQL_ENSURE = 0,1
SQL_FETCH_NEXT = 1
SQL_COLUMN_DISPLAY_SIZE = 6
SQL_INVALID_HANDLE = -2
SQL_NO_DATA_FOUND = 100; SQL_NULL_DATA = -1; SQL_NTS = -3
SQL_HANDLE_DESCR = 4
SQL_TABLE_NAMES = 3
SQL_PARAM_INPUT = 1; SQL_PARAM_INPUT_OUTPUT = 2
SQL_PARAM_TYPE_UNKNOWN = 0
SQL_RESULT_COL = 3
SQL_PARAM_OUTPUT = 4
SQL_RETURN_VALUE = 5
SQL_PARAM_TYPE_DEFAULT = SQL_PARAM_INPUT_OUTPUT

SQL_RESET_PARAMS = 3
SQL_UNBIND = 2
SQL_CLOSE = 0

#================================

SQL_QUALIFIER_LOCATION = 114
SQL_QUALIFIER_NAME_SEPARATOR = 41
SQL_QUALIFIER_TERM = 42
SQL_QUALIFIER_USAGE = 92
SQL_OWNER_TERM = 39
SQL_OWNER_USAGE = 91
SQL_ACCESSIBLE_PROCEDURES = 20
SQL_ACCESSIBLE_TABLES = 19
SQL_ACTIVE_ENVIRONMENTS = 116
SQL_AGGREGATE_FUNCTIONS = 169
SQL_ALTER_DOMAIN = 117
SQL_ALTER_TABLE = 86
SQL_ASYNC_MODE = 10021
SQL_BATCH_ROW_COUNT = 120
SQL_BATCH_SUPPORT = 121
SQL_BOOKMARK_PERSISTENCE = 82
SQL_CATALOG_LOCATION = SQL_QUALIFIER_LOCATION
SQL_CATALOG_NAME = 10003
SQL_CATALOG_NAME_SEPARATOR = SQL_QUALIFIER_NAME_SEPARATOR
SQL_CATALOG_TERM = SQL_QUALIFIER_TERM
SQL_CATALOG_USAGE = SQL_QUALIFIER_USAGE
SQL_COLLATION_SEQ = 10004
SQL_COLUMN_ALIAS = 87
SQL_CONCAT_NULL_BEHAVIOR = 22
SQL_CONVERT_FUNCTIONS = 48
SQL_CONVERT_VARCHAR = 70
SQL_CORRELATION_NAME = 74
SQL_CREATE_ASSERTION = 127
SQL_CREATE_CHARACTER_SET = 128
SQL_CREATE_COLLATION = 129
SQL_CREATE_DOMAIN = 130
SQL_CREATE_SCHEMA = 131
SQL_CREATE_TABLE = 132
SQL_CREATE_TRANSLATION = 133
SQL_CREATE_VIEW = 134
SQL_CURSOR_COMMIT_BEHAVIOR = 23
SQL_CURSOR_ROLLBACK_BEHAVIOR = 24
SQL_DATABASE_NAME = 16
SQL_DATA_SOURCE_NAME = 2
SQL_DATA_SOURCE_READ_ONLY = 25
SQL_DATETIME_LITERALS = 119
SQL_DBMS_NAME = 17
SQL_DBMS_VER = 18
SQL_DDL_INDEX = 170
SQL_DEFAULT_TXN_ISOLATION = 26
SQL_DESCRIBE_PARAMETER = 10002
SQL_DM_VER = 171
SQL_DRIVER_NAME = 6
SQL_DRIVER_ODBC_VER = 77
SQL_DRIVER_VER = 7
SQL_DROP_ASSERTION = 136
SQL_DROP_CHARACTER_SET = 137
SQL_DROP_COLLATION = 138
SQL_DROP_DOMAIN = 139
SQL_DROP_SCHEMA = 140
SQL_DROP_TABLE = 141
SQL_DROP_TRANSLATION = 142
SQL_DROP_VIEW = 143
SQL_DYNAMIC_CURSOR_ATTRIBUTES1 = 144
SQL_DYNAMIC_CURSOR_ATTRIBUTES2 = 145
SQL_EXPRESSIONS_IN_ORDERBY = 27
SQL_FILE_USAGE = 84
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 = 146
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2 = 147
SQL_GETDATA_EXTENSIONS = 81
SQL_GROUP_BY = 88
SQL_IDENTIFIER_CASE = 28
SQL_IDENTIFIER_QUOTE_CHAR = 29
SQL_INDEX_KEYWORDS = 148
SQL_INFO_SCHEMA_VIEWS = 149
SQL_INSERT_STATEMENT = 172
SQL_INTEGRITY = 73
SQL_KEYSET_CURSOR_ATTRIBUTES1 = 150
SQL_KEYSET_CURSOR_ATTRIBUTES2 = 151
SQL_KEYWORDS = 89
SQL_LIKE_ESCAPE_CLAUSE = 113
SQL_MAX_ASYNC_CONCURRENT_STATEMENTS = 10022
SQL_MAX_BINARY_LITERAL_LEN = 112
SQL_MAX_CATALOG_NAME_LEN = 34
SQL_MAX_CHAR_LITERAL_LEN = 108
SQL_MAX_COLUMNS_IN_GROUP_BY = 97
SQL_MAX_COLUMNS_IN_INDEX = 98
SQL_MAX_COLUMNS_IN_ORDER_BY = 99
SQL_MAX_COLUMNS_IN_SELECT = 100
SQL_MAX_COLUMNS_IN_TABLE = 101
SQL_MAX_COLUMN_NAME_LEN = 30
SQL_MAX_CONCURRENT_ACTIVITIES = 1
SQL_MAX_CURSOR_NAME_LEN = 31
SQL_MAX_DRIVER_CONNECTIONS = 0
SQL_MAX_IDENTIFIER_LEN = 10005
SQL_MAX_INDEX_SIZE = 102
SQL_MAX_PROCEDURE_NAME_LEN = 33
SQL_MAX_ROW_SIZE = 104
SQL_MAX_ROW_SIZE_INCLUDES_LONG = 103
SQL_MAX_SCHEMA_NAME_LEN = 32
SQL_MAX_STATEMENT_LEN = 105
SQL_MAX_TABLES_IN_SELECT = 106
SQL_MAX_TABLE_NAME_LEN = 35
SQL_MAX_USER_NAME_LEN = 107
SQL_MULTIPLE_ACTIVE_TXN = 37
SQL_MULT_RESULT_SETS = 36
SQL_NEED_LONG_DATA_LEN = 111
SQL_NON_NULLABLE_COLUMNS = 75
SQL_NULL_COLLATION = 85
SQL_NUMERIC_FUNCTIONS = 49
SQL_ODBC_INTERFACE_CONFORMANCE = 152
SQL_ODBC_VER = 10
SQL_OJ_CAPABILITIES = 65003
SQL_ORDER_BY_COLUMNS_IN_SELECT = 90
SQL_PARAM_ARRAY_ROW_COUNTS = 153
SQL_PARAM_ARRAY_SELECTS = 154
SQL_PROCEDURES = 21
SQL_PROCEDURE_TERM = 40
SQL_QUOTED_IDENTIFIER_CASE = 93
SQL_ROW_UPDATES = 11
SQL_SCHEMA_TERM = SQL_OWNER_TERM
SQL_SCHEMA_USAGE = SQL_OWNER_USAGE
SQL_SCROLL_OPTIONS = 44
SQL_SEARCH_PATTERN_ESCAPE = 14
SQL_SERVER_NAME = 13
SQL_SPECIAL_CHARACTERS = 94
SQL_SQL92_DATETIME_FUNCTIONS = 155
SQL_SQL92_FOREIGN_KEY_DELETE_RULE = 156
SQL_SQL92_FOREIGN_KEY_UPDATE_RULE = 157
SQL_SQL92_GRANT = 158
SQL_SQL92_NUMERIC_VALUE_FUNCTIONS = 159
SQL_SQL92_PREDICATES = 160
SQL_SQL92_RELATIONAL_JOIN_OPERATORS = 161
SQL_SQL92_REVOKE = 162
SQL_SQL92_ROW_VALUE_CONSTRUCTOR = 163
SQL_SQL92_STRING_FUNCTIONS = 164
SQL_SQL92_VALUE_EXPRESSIONS = 165
SQL_SQL_CONFORMANCE = 118
SQL_STANDARD_CLI_CONFORMANCE = 166
SQL_STATIC_CURSOR_ATTRIBUTES1 = 167
SQL_STATIC_CURSOR_ATTRIBUTES2 = 168
SQL_STRING_FUNCTIONS = 50
SQL_SUBQUERIES = 95
SQL_SYSTEM_FUNCTIONS = 51
SQL_TABLE_TERM = 45
SQL_TIMEDATE_ADD_INTERVALS = 109
SQL_TIMEDATE_DIFF_INTERVALS = 110
SQL_TIMEDATE_FUNCTIONS = 52
SQL_TXN_CAPABLE = 46
SQL_TXN_ISOLATION_OPTION = 72
SQL_UNION = 96
SQL_USER_NAME = 47
SQL_XOPEN_CLI_YEAR = 10000


SQL_TYPE_NULL = 0
SQL_DECIMAL = 3
SQL_FLOAT = 6
SQL_DATE = 9
SQL_TIME = 10
SQL_TIMESTAMP = 11
SQL_VARCHAR = 12
SQL_LONGVARCHAR = -1
SQL_VARBINARY = -3
SQL_LONGVARBINARY = -4
SQL_BIGINT = -5
SQL_WVARCHAR = -9
SQL_WLONGVARCHAR = -10
SQL_ALL_TYPES = 0
SQL_SIGNED_OFFSET = -20
SQL_SS_VARIANT = -150
SQL_SS_UDT = -151
SQL_SS_XML = -152
SQL_SS_TIME2 = -154

SQL_C_CHAR =            SQL_CHAR =          1
SQL_C_NUMERIC =         SQL_NUMERIC =       2
SQL_C_LONG =            SQL_INTEGER =       4
SQL_C_SLONG =           SQL_C_LONG + SQL_SIGNED_OFFSET
SQL_C_SHORT =           SQL_SMALLINT =      5
SQL_C_FLOAT =           SQL_REAL =          7
SQL_C_DOUBLE =          SQL_DOUBLE =        8
SQL_C_TYPE_DATE =       SQL_TYPE_DATE =     91
SQL_C_TYPE_TIME =       SQL_TYPE_TIME =     92
SQL_C_BINARY =          SQL_BINARY =        -2
SQL_C_SBIGINT =         SQL_BIGINT + SQL_SIGNED_OFFSET
SQL_C_TINYINT =         SQL_TINYINT =       -6
SQL_C_BIT =             SQL_BIT =           -7
SQL_C_WCHAR =           SQL_WCHAR =         -8
SQL_C_GUID =            SQL_GUID =          -11
SQL_C_TYPE_TIMESTAMP =  SQL_TYPE_TIMESTAMP = 93
SQL_C_DEFAULT = 99

SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3
SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_ERROR = 0, 1, -1
SQL_NO_DATA = 100; SQL_NO_TOTAL = -4
SQL_NO_DATA_FOUND = 100; SQL_NULL_DATA = -1; SQL_NTS = -3
SQL_INVALID_HANDLE = -2
SQL_RESET_PARAMS = 3
SQL_SS_TIME2 = -154
SQL_C_TYPE_DATE =       SQL_TYPE_DATE =     91
SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC2, SQL_OV_ODBC3 = 200, 2, 3
SQL_ATTR_CONNECTION_POOLING = 201; SQL_CP_ONE_PER_HENV = 2
SQL_C_TYPE_TIME =       SQL_TYPE_TIME =     92
SQL_C_BINARY =          SQL_BINARY =        -2
SQL_C_WCHAR =           SQL_WCHAR =         -8
SQL_ATTR_LOGIN_TIMEOUT = 103

SQL_DRIVER_NOPROMPT = 0
SQL_ATTR_ACCESS_MODE = SQL_ACCESS_MODE = 101
SQL_MODE_DEFAULT = SQL_MODE_READ_WRITE = 0; SQL_MODE_READ_ONLY = 1
SQL_IS_UINTEGER = -5

SQL_DESC_DISPLAY_SIZE = SQL_COLUMN_DISPLAY_SIZE

SQL_CLOSE = 0
SQL_C_SHORT =           SQL_SMALLINT =      5
SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3

py_ver = sys.version[:3]
py_v3 = py_ver >= '3.0'

unicode = str
long = int
BYTE_1 = bytes('1','ascii')

def dttm_cvt(x):
    if py_v3:
        x = x.decode('ascii')
    if x == '': return None
    else: return datetime.datetime(int(x[0:4]),int(x[5:7]),int(x[8:10]),int(x[10:13]),int(x[14:16]),int(x[17:19]),int(x[20:26].ljust(6,'0')[:6]))

def tm_cvt(x):
    if py_v3:
        x = x.decode('ascii')
    if x == '': return None
    else: return datetime.time(int(x[0:2]),int(x[3:5]),int(x[6:8]),int(x[9:].ljust(6,'0')[:6]))

def dt_cvt(x):
    if py_v3:
        x = x.decode('ascii')
    if x == '': return None
    else: return datetime.date(int(x[0:4]),int(x[5:7]),int(x[8:10]))

def Decimal_cvt(x):
    if py_v3:
        x = x.decode('ascii')
    return Decimal(x)

bytearray_cvt = bytearray
if sys.platform == 'cli':
    bytearray_cvt = lambda x: bytearray(buffer(x))


SQL_data_type_dict = { \

SQL_TYPE_NULL       : (None,                lambda x: None,             SQL_C_CHAR,         create_buffer,      2     ,         False         ),
SQL_CHAR            : (str,                 lambda x: x,                SQL_C_CHAR,         create_buffer,      2048  ,         False         ),
SQL_NUMERIC         : (Decimal,             Decimal_cvt,                SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_DECIMAL         : (Decimal,             Decimal_cvt,                SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_INTEGER         : (int,                 int,                        SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_SMALLINT        : (int,                 int,                        SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_FLOAT           : (float,               float,                      SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_REAL            : (float,               float,                      SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_DOUBLE          : (float,               float,                      SQL_C_CHAR,         create_buffer,      200   ,         False         ),
SQL_DATE            : (datetime.date,       dt_cvt,                     SQL_C_CHAR,         create_buffer,      30    ,         False         ),
SQL_TIME            : (datetime.time,       tm_cvt,                     SQL_C_CHAR,         create_buffer,      20    ,         False         ),
SQL_SS_TIME2        : (datetime.time,       tm_cvt,                     SQL_C_CHAR,         create_buffer,      20    ,         False         ),
SQL_TIMESTAMP       : (datetime.datetime,   dttm_cvt,                   SQL_C_CHAR,         create_buffer,      30    ,         False         ),
SQL_VARCHAR         : (str,                 lambda x: x,                SQL_C_CHAR,         create_buffer,      2048  ,         False         ),
SQL_LONGVARCHAR     : (str,                 lambda x: x,                SQL_C_CHAR,         create_buffer,      20500 ,         True          ),
SQL_BINARY          : (bytearray,           bytearray_cvt,              SQL_C_BINARY,       create_buffer,      5120  ,         True          ),
SQL_VARBINARY       : (bytearray,           bytearray_cvt,              SQL_C_BINARY,       create_buffer,      5120  ,         True          ),
SQL_LONGVARBINARY   : (bytearray,           bytearray_cvt,              SQL_C_BINARY,       create_buffer,      20500 ,         True          ),
SQL_BIGINT          : (long,                long,                       SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_TINYINT         : (int,                 int,                        SQL_C_CHAR,         create_buffer,      150   ,         False         ),
SQL_BIT             : (bool,                lambda x:x == BYTE_1,       SQL_C_CHAR,         create_buffer,      2     ,         False         ),
SQL_WCHAR           : (unicode,             lambda x: x,                SQL_C_WCHAR,        create_buffer_u,    2048  ,         False          ),
SQL_WVARCHAR        : (unicode,             lambda x: x,                SQL_C_WCHAR,        create_buffer_u,    2048  ,         False          ),
SQL_GUID            : (str,                 lambda x: x,                SQL_C_CHAR,         create_buffer,      2048  ,         False         ),
SQL_WLONGVARCHAR    : (unicode,             lambda x: x,                SQL_C_WCHAR,        create_buffer_u,    20500 ,         True          ),
SQL_TYPE_DATE       : (datetime.date,       dt_cvt,                     SQL_C_CHAR,         create_buffer,      30    ,         False         ),
SQL_TYPE_TIME       : (datetime.time,       tm_cvt,                     SQL_C_CHAR,         create_buffer,      20    ,         False         ),
SQL_TYPE_TIMESTAMP  : (datetime.datetime,   dttm_cvt,                   SQL_C_CHAR,         create_buffer,      30    ,         False         ),
SQL_SS_VARIANT      : (str,                 lambda x: x,                SQL_C_CHAR,         create_buffer,      2048  ,         True         ),
SQL_SS_XML          : (unicode,             lambda x: x,                SQL_C_WCHAR,        create_buffer_u,    20500 ,         True          ),
SQL_SS_UDT          : (bytearray,           bytearray_cvt,              SQL_C_BINARY,       create_buffer,      5120  ,         True          ),
}
