import sys, os, datetime, ctypes, threading
from decimal import Decimal


SQL_CLOSE = 0
ODBC_API = ctypes.windll.odbc32
SQL_HANDLE_STMT = 0, 1, 2, 3
ADDR = ctypes.byref

c_short = ctypes.c_short
buffer = memoryview
SQL_C_SHORT =           SQL_SMALLINT =      5

str_8b = bytes
from_buffer_u = lambda buffer: buffer.value
SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3
SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_ERROR = 0, 1, -1
SQL_NO_DATA = 100; SQL_NO_TOTAL = -4
SQL_NO_DATA_FOUND = 100; SQL_NULL_DATA = -1; SQL_NTS = -3
SQL_INVALID_HANDLE = -2
SQL_RESET_PARAMS = 3
SQL_C_TYPE_TIMESTAMP =  SQL_TYPE_TIMESTAMP = 93
UCS_buf = lambda s: s
SQLExecute = ODBC_API.SQLExecute
SQL_SS_TIME2 = -154
SQL_C_TYPE_DATE =       SQL_TYPE_DATE =     91
class BinaryNullType(): pass
BinaryNull = BinaryNullType()
SQL_C_TYPE_TIME =       SQL_TYPE_TIME =     92
def get_type(v):

    if isinstance(v, bool):
        return ('b',)
    elif isinstance(v, unicode):
        if len(v) >= 255:
            # use num of chars times 2 since utf-16-le encoding will double the number of bytes needed
            return  ('U',(len(v)//1000+1)*1000*2)
        else:
            return ('u',)
    elif isinstance(v, (str_8b,str)):
        if len(v) >= 255:
            return  ('S',(len(v)//1000+1)*1000)
        else:
            return  ('s',)
    elif isinstance(v, (int, long)):
        #SQL_BIGINT defination: http://msdn.microsoft.com/en-us/library/ms187745.aspx
        if v > 2147483647 or v < -2147483648:
            return  ('l',)
        else:
            return  ('i',)
    elif isinstance(v, float):
        return ('f',)
    elif isinstance(v, BinaryNullType):
        return ('BN',)
    elif v is None:
        return ('N',)
    elif isinstance(v, Decimal):
        t = v.as_tuple() #1.23 -> (1,2,3),-2 , 1.23*E7 -> (1,2,3),5
        return  ('D',(len(t[1]),0 - t[2])) # number of digits, and number of decimal digits


    elif isinstance (v, datetime.datetime):
        return ('dt',)
    elif isinstance (v, datetime.date):
        return ('d',)
    elif isinstance(v, datetime.time):
        return ('t',)
    elif isinstance (v, (bytearray, buffer)):
        return ('bi',(len(v)//1000+1)*1000)

    return type(v)


create_buffer = ctypes.create_string_buffer
create_buffer_u = ctypes.create_unicode_buffer

py_ver = sys.version[:3]
py_v3 = py_ver >= '3.0'

def TupleRow(cursor):
    """Normal tuple with added attribute `cursor_description`, as in pyodbc.
    This is the default.
    """
    class Row(tuple):
        cursor_description = cursor.description

        def get(self, field):
            if not hasattr(self, 'field_dict'):
                self.field_dict = self.to_dict()
            return self.field_dict.get(field)

        def to_dict(self):
            return {
                self.cursor_description[i][0]: item
                for i, item in enumerate(self)
            }

        def __getitem__(self, field):
            if isinstance(field, (unicode,str)):
                return self.get(field)
            else:
                return tuple.__getitem__(self,field)

    return

class OdbcNoLibrary(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class OdbcLibraryError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class OdbcInvalidHandle(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class OdbcGenericError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class Warning(Exception):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class Error(Exception):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class InterfaceError(Error):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class DatabaseError(Error):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class InternalError(DatabaseError):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class ProgrammingError(DatabaseError):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class DataError(DatabaseError):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class IntegrityError(DatabaseError):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class NotSupportedError(Error):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)
class OperationalError(DatabaseError):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)

def ctrl_err(ht, h, val_ret, ansi):
    """Classify type of ODBC error from (type of handle, handle, return value)
    , and raise with a list"""

    if ansi:
        state = create_buffer(22)
        Message = create_buffer(1024*4)
        ODBC_func = ODBC_API.SQLGetDiagRec
        if py_v3:
            raw_s = lambda s: bytes(s,'ascii')
        else:
            raw_s = str_8b
    else:
        state = create_buffer_u(24)
        Message = create_buffer_u(1024*4)
        ODBC_func = ODBC_API.SQLGetDiagRecW
        raw_s = unicode
    NativeError = ctypes.c_int()
    Buffer_len = c_short()
    err_list = []
    number_errors = 1

    while 1:
        ret = ODBC_func(ht, h, number_errors, state, \
            ADDR(NativeError), Message, 1024, ADDR(Buffer_len))
        if ret == SQL_NO_DATA_FOUND:
            #No more data, I can raise
            #print(err_list[0][1])
            state = err_list[0][0]
            err_text = raw_s('[')+state+raw_s('] ')+err_list[0][1]
            if state[:2] in (raw_s('24'),raw_s('25'),raw_s('42')):
                raise ProgrammingError(state,err_text)
            elif state[:2] in (raw_s('22')):
                raise DataError(state,err_text)
            elif state[:2] in (raw_s('23')) or state == raw_s('40002'):
                raise IntegrityError(state,err_text)
            elif state == raw_s('0A000'):
                raise NotSupportedError(state,err_text)
            elif state in (raw_s('HYT00'),raw_s('HYT01'),raw_s('01000')):
                raise OperationalError(state,err_text)
            elif state[:2] in (raw_s('IM'),raw_s('HY')):
                raise Error(state,err_text)
            else:
                raise DatabaseError(state,err_text)
            break
        elif ret == SQL_INVALID_HANDLE:
            #The handle passed is an invalid handle
            raise ProgrammingError('', 'SQL_INVALID_HANDLE')
        elif ret == SQL_SUCCESS:
            if ansi:
                err_list.append((state.value, Message.value, NativeError.value))
            else:
                err_list.append((from_buffer_u(state), from_buffer_u(Message), NativeError.value))
            number_errors += 1
        elif ret == SQL_ERROR:
            raise ProgrammingError('', 'SQL_ERROR')




def check_success(ODBC_obj, ret):
    """ Validate return value, if not success, raise exceptions based on the handle """
    if ret not in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA):
        if isinstance(ODBC_obj, Cursor):
            ctrl_err(SQL_HANDLE_STMT, ODBC_obj.stmt_h, ret, ODBC_obj.ansi)
        elif isinstance(ODBC_obj, Connection):
            ctrl_err(SQL_HANDLE_DBC, ODBC_obj.dbc_h, ret, ODBC_obj.ansi)
        else:
            ctrl_err(SQL_HANDLE_ENV, ODBC_obj, ret, False)


# The Cursor Class.
class Cursor:
    def __init__(self, conx, row_type_callable=None, lowercase=True):
        """ Initialize self.stmt_h, which is the handle of a statement
        A statement is actually the basis of a python"cursor" object
        """
        self.stmt_h = ctypes.c_void_p()
        self.connection = conx
        self.ansi = conx.ansi
        self.row_type_callable = row_type_callable or TupleRow
        self.statement = None
        self._last_param_types = None
        self._ParamBufferList = []
        self._ColBufferList = []
        self._row_type = None
        self._buf_cvt_func = []
        self.rowcount = -1
        self.description = None
        self.autocommit = None
        self._ColTypeCodeList = []
        self._outputsize = {}
        self._inputsizers = []
        self.arraysize = 1
        ret = ODBC_API.SQLAllocHandle(SQL_HANDLE_STMT, self.connection.dbc_h, ADDR(self.stmt_h))
        check_success(self, ret)

        self.timeout = conx.timeout
        if self.timeout != 0:
            self.set_timeout(self.timeout)
        self._PARAM_SQL_TYPE_LIST = []
        self.closed = False
        self.lowercase = lowercase

    # def set_timeout(self, timeout):
    #     self.timeout = timeout
    #     ret = ODBC_API.SQLSetStmtAttr(self.stmt_h, SQL_ATTR_QUERY_TIMEOUT, self.timeout, 0)
    #     check_success(self, ret)


    # def prepare(self, query_string):
    #     """prepare a query"""

    #     #self._free_results(FREE_STATEMENT)
    #     if not self.connection:
    #         self.close()

    #     if isinstance(query_string, unicode):
    #         c_query_string = wchar_pointer(UCS_buf(query_string))
    #         ret = ODBC_API.SQLPrepareW(self.stmt_h, c_query_string, len(query_string))
    #     else:
    #         c_query_string = ctypes.c_char_p(query_string)
    #         ret = ODBC_API.SQLPrepare(self.stmt_h, c_query_string, len(query_string))
    #     if ret != SQL_SUCCESS:
    #         check_success(self, ret)


    #     self._PARAM_SQL_TYPE_LIST = []

    #     if self.connection.support_SQLDescribeParam:
    #         # SQLServer's SQLDescribeParam only supports DML SQL, so avoid the SELECT statement
    #         if True:# 'SELECT' not in query_string.upper():
    #             #self._free_results(NO_FREE_STATEMENT)
    #             NumParams = c_short()
    #             ret = ODBC_API.SQLNumParams(self.stmt_h, ADDR(NumParams))
    #             if ret != SQL_SUCCESS:
    #                 check_success(self, ret)

    #             for col_num in range(NumParams.value):
    #                 ParameterNumber = ctypes.c_ushort(col_num + 1)
    #                 DataType = c_short()
    #                 ParameterSize = ctypes.c_size_t()
    #                 DecimalDigits = c_short()
    #                 Nullable = c_short()
    #                 ret = ODBC_API.SQLDescribeParam(
    #                     self.stmt_h,
    #                     ParameterNumber,
    #                     ADDR(DataType),
    #                     ADDR(ParameterSize),
    #                     ADDR(DecimalDigits),
    #                     ADDR(Nullable),
    #                 )
    #                 if ret != SQL_SUCCESS:
    #                     try:
    #                         check_success(self, ret)
    #                     except DatabaseError:
    #                         if sys.exc_info()[1].value[0] == '07009':
    #                             self._PARAM_SQL_TYPE_LIST = []
    #                             break
    #                         else:
    #                             raise sys.exc_info()[1]
    #                     except:
    #                         raise sys.exc_info()[1]

    #                 self._PARAM_SQL_TYPE_LIST.append((DataType.value,DecimalDigits.value))

    #     self.statement = query_string


    # def _BindParams(self, param_types, pram_io_list = []):
    #     """Create parameter buffers based on param types, and bind them to the statement"""
    #     # Clear the old Parameters
    #     if not self.connection:
    #         self.close()
    #     #self._free_results(NO_FREE_STATEMENT)

    #     # Get the number of query parameters judged by database.
    #     NumParams = c_short()
    #     ret = ODBC_API.SQLNumParams(self.stmt_h, ADDR(NumParams))
    #     if ret != SQL_SUCCESS:
    #         check_success(self, ret)

    #     if len(param_types) != NumParams.value:
    #         # In case number of parameters provided do not same as number required
    #         error_desc = "The SQL contains %d parameter markers, but %d parameters were supplied" \
    #                     %(NumParams.value,len(param_types))
    #         raise ProgrammingError('HY000',error_desc)


    #     # Every parameter needs to be binded to a buffer
    #     ParamBufferList = []
    #     # Temporary holder since we can only call SQLDescribeParam before
    #     # calling SQLBindParam.
    #     temp_holder = []
    #     for col_num in range(NumParams.value):
    #         dec_num = 0
    #         buf_size = 512

    #         if param_types[col_num][0] == 'u':
    #             sql_c_type = SQL_C_WCHAR
    #             sql_type = SQL_WVARCHAR
    #             # allocate two bytes for each char due to utf-16-le encoding
    #             buf_size = 255 * 2
    #             ParameterBuffer = create_buffer_u(buf_size)

    #         elif param_types[col_num][0] == 's':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_VARCHAR
    #             buf_size = 255
    #             ParameterBuffer = create_buffer(buf_size)


    #         elif param_types[col_num][0] == 'U':
    #             sql_c_type = SQL_C_WCHAR
    #             sql_type = SQL_WLONGVARCHAR
    #             buf_size = param_types[col_num][1]#len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
    #             ParameterBuffer = create_buffer_u(buf_size)

    #         elif param_types[col_num][0] == 'S':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_LONGVARCHAR
    #             buf_size = param_types[col_num][1]#len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
    #             ParameterBuffer = create_buffer(buf_size)

    #         # bool subclasses int, thus has to go first
    #         elif param_types[col_num][0] == 'b':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_BIT
    #             buf_size = SQL_data_type_dict[sql_type][4]
    #             ParameterBuffer = create_buffer(buf_size)

    #         elif param_types[col_num][0] == 'i':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_INTEGER
    #             buf_size = SQL_data_type_dict[sql_type][4]
    #             ParameterBuffer = create_buffer(buf_size)

    #         elif param_types[col_num][0] == 'l':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_BIGINT
    #             buf_size = SQL_data_type_dict[sql_type][4]
    #             ParameterBuffer = create_buffer(buf_size)


    #         elif param_types[col_num][0] == 'D': #Decimal
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_NUMERIC
    #             digit_num, dec_num = param_types[col_num][1]
    #             if dec_num > 0:
    #                 # has decimal
    #                 # 1.23 as_tuple -> (1,2,3),-2
    #                 # 1.23 digit_num = 3 dec_num = 2
    #                 # 0.11 digit_num = 2 dec_num = 2
    #                 # 0.01 digit_num = 1 dec_num = 2
    #                 if dec_num > digit_num:
    #                     buf_size = dec_num
    #                 else:
    #                     buf_size = digit_num
    #                     #dec_num = dec_num
    #             else:
    #                 # no decimal
    #                 buf_size = digit_num - dec_num
    #                 dec_num = 0

    #             ParameterBuffer = create_buffer(buf_size + 4)# add extra length for sign and dot


    #         elif param_types[col_num][0] == 'f':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_DOUBLE
    #             buf_size = SQL_data_type_dict[sql_type][4]
    #             ParameterBuffer = create_buffer(buf_size)


    #         # datetime subclasses date, thus has to go first
    #         elif param_types[col_num][0] == 'dt':
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_TYPE_TIMESTAMP
    #             buf_size = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
    #             ParameterBuffer = create_buffer(buf_size)
    #             dec_num = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][1]


    #         elif param_types[col_num][0] == 'd':
    #             sql_c_type = SQL_C_CHAR
    #             if SQL_TYPE_DATE in self.connection.type_size_dic:
    #                 #if DEBUG:print('conx.type_size_dic.has_key(SQL_TYPE_DATE)')
    #                 sql_type = SQL_TYPE_DATE
    #                 buf_size = self.connection.type_size_dic[SQL_TYPE_DATE][0]

    #                 ParameterBuffer = create_buffer(buf_size)
    #                 dec_num = self.connection.type_size_dic[SQL_TYPE_DATE][1] or 0

    #             else:
    #                 # SQL Sever <2008 doesn't have a DATE type.
    #                 sql_type = SQL_TYPE_TIMESTAMP
    #                 buf_size, dec_num = 10, 0
    #                 ParameterBuffer = create_buffer(buf_size)


    #         elif param_types[col_num][0] == 't':
    #             sql_c_type = SQL_C_CHAR
    #             if SQL_TYPE_TIME in self.connection.type_size_dic:
    #                 sql_type = SQL_TYPE_TIME
    #                 buf_size = self.connection.type_size_dic[SQL_TYPE_TIME][0]
    #                 ParameterBuffer = create_buffer(buf_size)
    #                 dec_num = self.connection.type_size_dic[SQL_TYPE_TIME][1]
    #             elif SQL_SS_TIME2 in self.connection.type_size_dic:
    #                 # TIME type added in SQL Server 2008
    #                 sql_type = SQL_SS_TIME2
    #                 buf_size = self.connection.type_size_dic[SQL_SS_TIME2][0]
    #                 ParameterBuffer = create_buffer(buf_size)
    #                 dec_num = self.connection.type_size_dic[SQL_SS_TIME2][1]
    #             else:
    #                 # SQL Sever <2008 doesn't have a TIME type.
    #                 sql_type = SQL_TYPE_TIMESTAMP
    #                 buf_size = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
    #                 ParameterBuffer = create_buffer(buf_size)
    #                 dec_num = 3

    #         elif param_types[col_num][0] == 'BN':
    #             sql_c_type = SQL_C_BINARY
    #             sql_type = SQL_VARBINARY
    #             buf_size = 1
    #             ParameterBuffer = create_buffer(buf_size)

    #         elif param_types[col_num][0] == 'N':
    #             if len(self._PARAM_SQL_TYPE_LIST) > 0:
    #                 sql_c_type = SQL_C_DEFAULT
    #                 sql_type = self._PARAM_SQL_TYPE_LIST[col_num][0]
    #                 buf_size = 1
    #                 ParameterBuffer = create_buffer(buf_size)
    #             else:
    #                 sql_c_type = SQL_C_CHAR
    #                 sql_type = SQL_CHAR
    #                 buf_size = 1
    #                 ParameterBuffer = create_buffer(buf_size)
    #         elif param_types[col_num][0] == 'bi':
    #             sql_c_type = SQL_C_BINARY
    #             sql_type = SQL_LONGVARBINARY
    #             buf_size = param_types[col_num][1]#len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
    #             ParameterBuffer = create_buffer(buf_size)


    #         else:
    #             sql_c_type = SQL_C_CHAR
    #             sql_type = SQL_LONGVARCHAR
    #             buf_size = len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
    #             ParameterBuffer = create_buffer(buf_size)

    #         temp_holder.append((sql_c_type, sql_type, buf_size, dec_num, ParameterBuffer))

    #     for col_num, (sql_c_type, sql_type, buf_size, dec_num, ParameterBuffer) in enumerate(temp_holder):
    #         BufferLen = c_ssize_t(buf_size)
    #         LenOrIndBuf = c_ssize_t()


    #         InputOutputType = SQL_PARAM_INPUT
    #         if len(pram_io_list) > col_num:
    #             InputOutputType = pram_io_list[col_num]

    #         ret = SQLBindParameter(self.stmt_h, col_num + 1, InputOutputType, sql_c_type, sql_type, buf_size,\
    #                 dec_num, ADDR(ParameterBuffer), BufferLen,ADDR(LenOrIndBuf))
    #         if ret != SQL_SUCCESS:
    #             check_success(self, ret)
    #         # Append the value buffer and the length buffer to the array
    #         ParamBufferList.append((ParameterBuffer,LenOrIndBuf,sql_type))

    #     self._last_param_types = param_types
    #     self._ParamBufferList = ParamBufferList


    def execute(self, query_string, params=None, many_mode=False, call_mode=False):
        """ Execute the query string, with optional parameters.
        If parameters are provided, the query would first be prepared, then executed with parameters;
        If parameters are not provided, only th query sting, it would be executed directly
        """
        if not self.connection:
            self.close()

        self._free_stmt(SQL_CLOSE)
        if params:
            # If parameters exist, first prepare the query then executed with parameters

            if not isinstance(params, (tuple, list)):
                raise TypeError("Params must be in a list, tuple, or Row")


            if query_string != self.statement:
                # if the query is not same as last query, then it is not prepared
                self.prepare(query_string)


            param_types = list(map(get_type, params))

            if call_mode:
                self._free_stmt(SQL_RESET_PARAMS)
                self._BindParams(param_types, self._pram_io_list)
            else:
                if self._last_param_types is None:
                    self._free_stmt(SQL_RESET_PARAMS)
                    self._BindParams(param_types)
                elif len(param_types) != len(self._last_param_types):
                    self._free_stmt(SQL_RESET_PARAMS)
                    self._BindParams(param_types)
                elif sum([p_type[0] != 'N' and p_type != self._last_param_types[i] for i,p_type in enumerate(param_types)]) > 0:
                    self._free_stmt(SQL_RESET_PARAMS)
                    self._BindParams(param_types)


            # With query prepared, now put parameters into buffers
            col_num = 0
            for param_buffer, param_buffer_len, sql_type in self._ParamBufferList:
                c_char_buf, c_buf_len = '', 0
                param_val = params[col_num]
                if param_types[col_num][0] in ('N','BN'):
                    param_buffer_len.value = SQL_NULL_DATA
                    col_num += 1
                    continue
                elif param_types[col_num][0] in ('i','l','f'):
                    if py_v3:
                        c_char_buf = bytes(str(param_val),'ascii')
                    else:
                        c_char_buf = str(param_val)
                    c_buf_len = len(c_char_buf)

                elif param_types[col_num][0] in ('s','S'):
                    c_char_buf = param_val
                    c_buf_len = len(c_char_buf)
                elif param_types[col_num][0] in ('u','U'):
                    c_char_buf = UCS_buf(param_val)
                    c_buf_len = len(c_char_buf)

                elif param_types[col_num][0] == 'dt':
                    max_len = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
                    datetime_str = param_val.strftime('%Y-%m-%d %H:%M:%S.%f')
                    c_char_buf = datetime_str[:max_len]
                    if py_v3:
                        c_char_buf = bytes(c_char_buf,'ascii')

                    c_buf_len = len(c_char_buf)
                    # print c_buf_len, c_char_buf

                elif param_types[col_num][0] == 'd':
                    if SQL_TYPE_DATE in self.connection.type_size_dic:
                        max_len = self.connection.type_size_dic[SQL_TYPE_DATE][0]
                    else:
                        max_len = 10
                    c_char_buf = param_val.isoformat()[:max_len]
                    if py_v3:
                        c_char_buf = bytes(c_char_buf,'ascii')
                    c_buf_len = len(c_char_buf)
                    #print c_char_buf

                elif param_types[col_num][0] == 't':
                    if SQL_TYPE_TIME in self.connection.type_size_dic:
                        max_len = self.connection.type_size_dic[SQL_TYPE_TIME][0]
                        c_char_buf = param_val.isoformat()[:max_len]
                        c_buf_len = len(c_char_buf)
                    elif SQL_SS_TIME2 in self.connection.type_size_dic:
                        max_len = self.connection.type_size_dic[SQL_SS_TIME2][0]
                        c_char_buf = param_val.isoformat()[:max_len]
                        c_buf_len = len(c_char_buf)
                    else:
                        c_buf_len = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
                        time_str = param_val.isoformat()
                        if len(time_str) == 8:
                            time_str += '.000'
                        c_char_buf = '1900-01-01 '+time_str[0:c_buf_len - 11]
                    if py_v3:
                        c_char_buf = bytes(c_char_buf,'ascii')
                    #print c_buf_len, c_char_buf

                elif param_types[col_num][0] == 'b':
                    if param_val == True:
                        c_char_buf = '1'
                    else:
                        c_char_buf = '0'
                    if py_v3:
                        c_char_buf = bytes(c_char_buf,'ascii')
                    c_buf_len = 1

                elif param_types[col_num][0] == 'D': #Decimal
                    sign = param_val.as_tuple()[0] == 0 and '+' or '-'
                    digit_string = ''.join([str(x) for x in param_val.as_tuple()[1]])
                    digit_num, dec_num = param_types[col_num][1]
                    if dec_num > 0:
                        # has decimal
                        # 1.12 digit_num = 3 dec_num = 2
                        # 0.11 digit_num = 2 dec_num = 2
                        # 0.01 digit_num = 1 dec_num = 2
                        left_part = digit_string[:digit_num - dec_num]
                        right_part = digit_string[0-dec_num:].zfill(dec_num)
                        v = ''.join((sign, left_part,'.', right_part))
                    else:
                        # no decimal
                        v = ''.join((digit_string, '0' * (0 - dec_num)))

                    if py_v3:
                        c_char_buf = bytes(v,'ascii')
                    else:
                        c_char_buf = v
                    c_buf_len = len(c_char_buf)

                elif param_types[col_num][0] == 'bi':
                    c_char_buf = str_8b(param_val)
                    c_buf_len = len(c_char_buf)

                else:
                    c_char_buf = param_val


                if param_types[col_num][0] == 'bi':
                    param_buffer.raw = str_8b(param_val)

                else:
                    #print (type(param_val),param_buffer, param_buffer.value)
                    param_buffer.value = c_char_buf

                if param_types[col_num][0] in ('U','u','S','s'):
                    #ODBC driver will find NUL in unicode and string to determine their length
                    param_buffer_len.value = SQL_NTS
                else:
                    param_buffer_len.value = c_buf_len

                col_num += 1
            ret = SQLExecute(self.stmt_h)
            if ret != SQL_SUCCESS:
                #print param_valparam_buffer, param_buffer.value
                check_success(self, ret)


            if not many_mode:
                self._NumOfRows()
                self._UpdateDesc()
                #self._BindCols()

        else:
            self.execdirect(query_string)
        return self


#     def _SQLExecute(self):
#         if not self.connection:
#             self.close()
#         ret = SQLExecute(self.stmt_h)
#         if ret != SQL_SUCCESS:
#             check_success(self, ret)


#     def execdirect(self, query_string):
#         """Execute a query directly"""
#         if not self.connection:
#             self.close()

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None
#         if isinstance(query_string, unicode):
#             c_query_string = wchar_pointer(UCS_buf(query_string))
#             ret = ODBC_API.SQLExecDirectW(self.stmt_h, c_query_string, len(query_string))
#         else:
#             c_query_string = ctypes.c_char_p(query_string)
#             ret = ODBC_API.SQLExecDirect(self.stmt_h, c_query_string, len(query_string))
#         check_success(self, ret)
#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def callproc(self, procname, args):
#         if not self.connection:
#             self.close()
#         raise Warning('', 'Still not fully implemented')
#         self._pram_io_list = [row[4] for row in self.procedurecolumns(procedure = procname).fetchall() if row[4] not in (SQL_RESULT_COL, SQL_RETURN_VALUE)]

#         print('pram_io_list: '+str(self._pram_io_list))



#         call_escape = '{CALL '+procname
#         if args:
#             call_escape += '(' + ','.join(['?' for params in args]) + ')'
#         call_escape += '}'

#         self.execute(call_escape, args, call_mode = True)

#         result = []

#         for buf, buf_len, sql_type in self._ParamBufferList:
#             if buf_len.value == -1:
#                 result.append(None)
#             else:
#                 result.append(self.connection.output_converter[sql_type](buf.value))
#         return result



#     def executemany(self, query_string, params_list = [None]):
#         if not self.connection:
#             self.close()

#         for params in params_list:
#             self.execute(query_string, params, many_mode = True)
#         self._NumOfRows()
#         self.rowcount = -1
#         self._UpdateDesc()
#         #self._BindCols()



#     def _CreateColBuf(self):
#         if not self.connection:
#             self.close()
#         self._free_stmt(SQL_UNBIND)
#         NOC = self._NumOfCols()
#         self._ColBufferList = []
#         bind_data = True
#         for col_num in range(NOC):
#             col_name = self.description[col_num][0]
#             col_size = self.description[col_num][2]
#             col_sql_data_type = self._ColTypeCodeList[col_num]

#             target_type = SQL_data_type_dict[col_sql_data_type][2]
#             dynamic_length = SQL_data_type_dict[col_sql_data_type][5]
#             # set default size base on the column's sql data type
#             total_buf_len = SQL_data_type_dict[col_sql_data_type][4]

#             # over-write if there's pre-set size value for "large columns"
#             if total_buf_len > 20500:
#                 total_buf_len = self._outputsize.get(None,total_buf_len)
#             # over-write if there's pre-set size value for the "col_num" column
#             total_buf_len = self._outputsize.get(col_num, total_buf_len)

#             # if the size of the buffer is very long, do not bind
#             # because a large buffer decrease performance, and sometimes you only get a NULL value.
#             # in that case use sqlgetdata instead.
#             if col_size >= 1024:
#                 dynamic_length = True

#             alloc_buffer = SQL_data_type_dict[col_sql_data_type][3](total_buf_len)

#             used_buf_len = c_ssize_t()

#             force_unicode = self.connection.unicode_results

#             if force_unicode and col_sql_data_type in (SQL_CHAR,SQL_VARCHAR,SQL_LONGVARCHAR):
#                 target_type = SQL_C_WCHAR
#                 alloc_buffer = create_buffer_u(total_buf_len)

#             buf_cvt_func = self.connection.output_converter[self._ColTypeCodeList[col_num]]

#             if bind_data:
#                 if dynamic_length:
#                     bind_data = False
#             self._ColBufferList.append([col_name, target_type, used_buf_len, ADDR(used_buf_len), alloc_buffer, ADDR(alloc_buffer), total_buf_len, buf_cvt_func, bind_data])

#             if bind_data:
#                 ret = ODBC_API.SQLBindCol(self.stmt_h, col_num + 1, target_type, ADDR(alloc_buffer), total_buf_len, ADDR(used_buf_len))
#                 if ret != SQL_SUCCESS:
#                     check_success(self, ret)

#     def _UpdateDesc(self):
#         "Get the information of (name, type_code, display_size, internal_size, col_precision, scale, null_ok)"
#         if not self.connection:
#             self.close()

#         force_unicode = self.connection.unicode_results
#         if force_unicode:
#             Cname = create_buffer_u(1024)
#         else:
#             Cname = create_buffer(1024)

#         Cname_ptr = c_short()
#         Ctype_code = c_short()
#         Csize = ctypes.c_size_t()
#         Cdisp_size = c_ssize_t(0)
#         CDecimalDigits = c_short()
#         Cnull_ok = c_short()
#         ColDescr = []
#         self._ColTypeCodeList = []
#         NOC = self._NumOfCols()
#         for col in range(1, NOC+1):

#             ret = ODBC_API.SQLColAttribute(self.stmt_h, col, SQL_DESC_DISPLAY_SIZE, ADDR(create_buffer(10)),
#                 10, ADDR(c_short()),ADDR(Cdisp_size))
#             if ret != SQL_SUCCESS:
#                 check_success(self, ret)

#             if force_unicode:

#                 ret = ODBC_API.SQLDescribeColW(self.stmt_h, col, Cname, len(Cname), ADDR(Cname_ptr),\
#                     ADDR(Ctype_code),ADDR(Csize),ADDR(CDecimalDigits), ADDR(Cnull_ok))
#                 if ret != SQL_SUCCESS:
#                     check_success(self, ret)
#             else:

#                 ret = ODBC_API.SQLDescribeCol(self.stmt_h, col, Cname, len(Cname), ADDR(Cname_ptr),\
#                     ADDR(Ctype_code),ADDR(Csize),ADDR(CDecimalDigits), ADDR(Cnull_ok))
#                 if ret != SQL_SUCCESS:
#                     check_success(self, ret)

#             col_name = from_buffer_u(Cname)
#             if self.lowercase:
#                 col_name = col_name.lower()
#             #(name, type_code, display_size,

#             ColDescr.append((col_name, SQL_data_type_dict.get(Ctype_code.value,(Ctype_code.value,))[0],Cdisp_size.value,\
#                 Csize.value, Csize.value,CDecimalDigits.value,Cnull_ok.value == 1 and True or False))
#             self._ColTypeCodeList.append(Ctype_code.value)

#         if len(ColDescr) > 0:
#             self.description = ColDescr
#             # Create the row type before fetching.
#             self._row_type = self.row_type_callable(self)
#         else:
#             self.description = None
#         self._CreateColBuf()


#     def _NumOfRows(self):
#         """Get the number of rows"""
#         if not self.connection:
#             self.close()

#         NOR = c_ssize_t()
#         ret = SQLRowCount(self.stmt_h, ADDR(NOR))
#         if ret != SQL_SUCCESS:
#             check_success(self, ret)
#         self.rowcount = NOR.value
#         return self.rowcount


#     def _NumOfCols(self):
#         """Get the number of cols"""
#         if not self.connection:
#             self.close()

#         NOC = c_short()
#         ret = SQLNumResultCols(self.stmt_h, ADDR(NOC))
#         if ret != SQL_SUCCESS:
#             check_success(self, ret)
#         return NOC.value


#     def fetchall(self):
#         if not self.connection:
#             self.close()

#         rows = []
#         while True:
#             row = self.fetchone()
#             if row is None:
#                 break
#             rows.append(row)
#         return rows


#     def fetchmany(self, num = None):
#         if not self.connection:
#             self.close()

#         if num is None:
#             num = self.arraysize
#         rows = []

#         while len(rows) < num:
#             row = self.fetchone()
#             if row is None:
#                 break
#             rows.append(row)
#         return rows


#     def fetchone(self):
#         if not self.connection:
#             self.close()

#         ret = SQLFetch(self.stmt_h)

#         if ret in (SQL_SUCCESS,SQL_SUCCESS_WITH_INFO):
#             '''Bind buffers for the record set columns'''

#             value_list = []
#             col_num = 1
#             for col_name, target_type, used_buf_len, ADDR_used_buf_len, alloc_buffer, ADDR_alloc_buffer, total_buf_len, buf_cvt_func, bind_data in self._ColBufferList:
#                 raw_data_parts = []
#                 while 1:
#                     if bind_data:
#                         ret = SQL_SUCCESS
#                     else:
#                         ret = SQLGetData(self.stmt_h, col_num, target_type, ADDR_alloc_buffer, total_buf_len, ADDR_used_buf_len)
#                     if ret == SQL_SUCCESS:
#                         if used_buf_len.value == SQL_NULL_DATA:
#                             value_list.append(None)
#                         else:
#                             if raw_data_parts == []:
#                                 # Means no previous data, no need to combine
#                                 if target_type == SQL_C_BINARY:
#                                     value_list.append(buf_cvt_func(alloc_buffer.raw[:used_buf_len.value]))
#                                 elif target_type == SQL_C_WCHAR:
#                                     if used_buf_len.value < total_buf_len:
#                                         ctypes.memset(ctypes.addressof(alloc_buffer) + used_buf_len.value, 0, 1)
#                                     value_list.append(buf_cvt_func(from_buffer_u(alloc_buffer)))
#                                 elif alloc_buffer.value == '':
#                                     value_list.append('')
#                                 else:
#                                     value_list.append(buf_cvt_func(alloc_buffer.value))
#                             else:
#                                 # There are previous fetched raw data to combine
#                                 if target_type == SQL_C_BINARY:
#                                     raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
#                                 elif target_type == SQL_C_WCHAR:
#                                     raw_data_parts.append(from_buffer_u(alloc_buffer))
#                                 else:
#                                     raw_data_parts.append(alloc_buffer.value)
#                         break

#                     elif ret == SQL_SUCCESS_WITH_INFO:
#                         # Means the data is only partial
#                         if target_type == SQL_C_BINARY:
#                             raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
#                         elif target_type == SQL_C_WCHAR:
#                             raw_data_parts.append(from_buffer_u(alloc_buffer))
#                         else:
#                             raw_data_parts.append(alloc_buffer.value)

#                     elif ret == SQL_NO_DATA:
#                         # Means all data has been transmitted
#                         break
#                     else:
#                         check_success(self, ret)

#                 if raw_data_parts != []:
#                     if py_v3:
#                         if target_type != SQL_C_BINARY:
#                             data_parts = [x.decode("utf-8") if type(x) is bytes else x for x in raw_data_parts]
#                             raw_value = ''.join(data_parts)
#                         else:
#                             raw_value = BLANK_BYTE.join(raw_data_parts)
#                     else:
#                         raw_value = ''.join(raw_data_parts)

#                     value_list.append(buf_cvt_func(raw_value))
#                 col_num += 1

#             return self._row_type(value_list)

#         else:
#             if ret == SQL_NO_DATA_FOUND:

#                 return None
#             else:
#                 check_success(self, ret)

#     def __next__(self):
#         return self.next()

#     def next(self):
#         row = self.fetchone()
#         if row is None:
#             raise(StopIteration)
#         return row

#     def __iter__(self):
#         return self


#     def skip(self, count = 0):
#         if not self.connection:
#             self.close()

#         for i in range(count):
#             ret = ODBC_API.SQLFetchScroll(self.stmt_h, SQL_FETCH_NEXT, 0)
#             if ret != SQL_SUCCESS:
#                 check_success(self, ret)
#         return None



#     def nextset(self):
#         if not self.connection:
#             self.close()

#         ret = ODBC_API.SQLMoreResults(self.stmt_h)
#         if ret not in (SQL_SUCCESS, SQL_NO_DATA):
#             check_success(self, ret)

#         if ret == SQL_NO_DATA:
#             self._free_stmt()
#             return False
#         else:
#             self._NumOfRows()
#             self._UpdateDesc()
#             #self._BindCols()
#         return True


#     def _free_stmt(self, free_type = None):
#         if not self.connection:
#             self.close()

#         if not self.connection.connected:
#             raise ProgrammingError('HY000','Attempt to use a closed connection.')

#         #self.description = None
#         #self.rowcount = -1
#         if free_type in (SQL_CLOSE, None):
#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_CLOSE)
#             if ret != SQL_SUCCESS:
#                 check_success(self, ret)
#         if free_type in (SQL_UNBIND, None):
#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_UNBIND)
#             if ret != SQL_SUCCESS:
#                 check_success(self, ret)
#         if free_type in (SQL_RESET_PARAMS, None):
#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_RESET_PARAMS)
#             if ret != SQL_SUCCESS:
#                 check_success(self, ret)



#     def getTypeInfo(self, sqlType = None):
#         if not self.connection:
#             self.close()

#         if sqlType is None:
#             type = SQL_ALL_TYPES
#         else:
#             type = sqlType
#         ret = ODBC_API.SQLGetTypeInfo(self.stmt_h, type)
#         if ret in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO):
#             self._NumOfRows()
#             self._UpdateDesc()
#             #self._BindCols()
#             return self.fetchone()


#     def tables(self, table=None, catalog=None, schema=None, tableType=None):
#         """Return a list with all tables"""
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_table = l_tableType = 0

#         if any(isinstance(x, unicode) for x in (table, catalog, schema, tableType)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLTablesW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLTables



#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)

#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)

#         if table is not None:
#             l_table = len(table)
#             table = string_p(table)

#         if tableType is not None:
#             l_tableType = len(tableType)
#             tableType = string_p(tableType)

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None
#         ret = API_f(self.stmt_h,
#                                 catalog, l_catalog,
#                                 schema, l_schema,
#                                 table, l_table,
#                                 tableType, l_tableType)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def columns(self, table=None, catalog=None, schema=None, column=None):
#         """Return a list with all columns"""
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_table = l_column = 0

#         if any(isinstance(x, unicode) for x in (table, catalog, schema, column)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLColumnsW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLColumns



#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)
#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)
#         if table is not None:
#             l_table = len(table)
#             table = string_p(table)
#         if column is not None:
#             l_column = len(column)
#             column = string_p(column)

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     table, l_table,
#                     column, l_column)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def primaryKeys(self, table=None, catalog=None, schema=None):
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_table = 0

#         if any(isinstance(x, unicode) for x in (table, catalog, schema)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLPrimaryKeysW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLPrimaryKeys



#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)

#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)

#         if table is not None:
#             l_table = len(table)
#             table = string_p(table)

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     table, l_table)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def foreignKeys(self, table=None, catalog=None, schema=None, foreignTable=None, foreignCatalog=None, foreignSchema=None):
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_table = l_foreignTable = l_foreignCatalog = l_foreignSchema = 0

#         if any(isinstance(x, unicode) for x in (table, catalog, schema, foreignTable, foreignCatalog, foreignSchema)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLForeignKeysW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLForeignKeys

#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)
#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)
#         if table is not None:
#             l_table = len(table)
#             table = string_p(table)
#         if foreignTable is not None:
#             l_foreignTable = len(foreignTable)
#             foreignTable = string_p(foreignTable)
#         if foreignCatalog is not None:
#             l_foreignCatalog = len(foreignCatalog)
#             foreignCatalog = string_p(foreignCatalog)
#         if foreignSchema is not None:
#             l_foreignSchema = len(foreignSchema)
#             foreignSchema = string_p(foreignSchema)

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     table, l_table,
#                     foreignCatalog, l_foreignCatalog,
#                     foreignSchema, l_foreignSchema,
#                     foreignTable, l_foreignTable)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def procedurecolumns(self, procedure=None, catalog=None, schema=None, column=None):
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_procedure = l_column = 0
#         if any(isinstance(x, unicode) for x in (procedure, catalog, schema, column)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLProcedureColumnsW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLProcedureColumns


#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)
#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)
#         if procedure is not None:
#             l_procedure = len(procedure)
#             procedure = string_p(procedure)
#         if column is not None:
#             l_column = len(column)
#             column = string_p(column)


#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     procedure, l_procedure,
#                     column, l_column)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         return self


#     def procedures(self, procedure=None, catalog=None, schema=None):
#         if not self.connection:
#             self.close()

#         l_catalog = l_schema = l_procedure = 0

#         if any(isinstance(x, unicode) for x in (procedure, catalog, schema)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLProceduresW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLProcedures



#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)
#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)
#         if procedure is not None:
#             l_procedure = len(procedure)
#             procedure = string_p(procedure)


#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     procedure, l_procedure)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         return self


#     def statistics(self, table, catalog=None, schema=None, unique=False, quick=True):
#         if not self.connection:
#             self.close()

#         l_table = l_catalog = l_schema = 0

#         if any(isinstance(x, unicode) for x in (table, catalog, schema)):
#             string_p = lambda x:wchar_pointer(UCS_buf(x))
#             API_f = ODBC_API.SQLStatisticsW
#         else:
#             string_p = ctypes.c_char_p
#             API_f = ODBC_API.SQLStatistics


#         if catalog is not None:
#             l_catalog = len(catalog)
#             catalog = string_p(catalog)
#         if schema is not None:
#             l_schema = len(schema)
#             schema = string_p(schema)
#         if table is not None:
#             l_table = len(table)
#             table = string_p(table)

#         if unique:
#             Unique = SQL_INDEX_UNIQUE
#         else:
#             Unique = SQL_INDEX_ALL
#         if quick:
#             Reserved = SQL_QUICK
#         else:
#             Reserved = SQL_ENSURE

#         self._free_stmt()
#         self._last_param_types = None
#         self.statement = None

#         ret = API_f(self.stmt_h,
#                     catalog, l_catalog,
#                     schema, l_schema,
#                     table, l_table,
#                     Unique, Reserved)
#         check_success(self, ret)

#         self._NumOfRows()
#         self._UpdateDesc()
#         #self._BindCols()
#         return self


#     def commit(self):
#         if not self.connection:
#             self.close()
#         self.connection.commit()

#     def rollback(self):
#         if not self.connection:
#             self.close()
#         self.connection.rollback()

#     def setoutputsize(self, size, column = None):
#         if not self.connection:
#             self.close()
#         self._outputsize[column] = size

#     def setinputsizes(self, sizes):
#         if not self.connection:
#             self.close()
#         self._inputsizers = [size for size in sizes]


#     def close(self):
#         """ Call SQLCloseCursor API to free the statement handle"""
# #        ret = ODBC_API.SQLCloseCursor(self.stmt_h)
# #        check_success(self, ret)
# #
#         if self.connection.connected:
#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_CLOSE)
#             check_success(self, ret)

#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_UNBIND)
#             check_success(self, ret)

#             ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_RESET_PARAMS)
#             check_success(self, ret)

#             ret = ODBC_API.SQLFreeHandle(SQL_HANDLE_STMT, self.stmt_h)
#             check_success(self, ret)


#         self.closed = True


#     def __del__(self):
#         if not self.closed:
#             self.close()

#     def __exit__(self, type, value, traceback):
#         if not self.connection:
#             self.close()

#         if value:
#             self.rollback()
#         else:
#             self.commit()

#         self.close()


#     def __enter__(self):
#         return self
