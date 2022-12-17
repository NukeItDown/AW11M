import sys, datetime, threading
from decimal import Decimal
from SQLtypes import *

ODBC_API = ctypes.windll.odbc32
use_unicode = True
connection_timeout = 0
wchar_pointer = ctypes.c_wchar_p
UCS_buf = lambda s: s
c_ssize_t = ctypes.c_ssize_t
SQLNumResultCols = ODBC_API.SQLNumResultCols

SQLEndTran = ODBC_API.SQLEndTran
SQLRowCount = ODBC_API.SQLRowCount

bytearray_cvt = bytearray
if sys.platform == 'cli':
    bytearray_cvt = lambda x: bytearray(buffer(x))

lock = threading.Lock()

#================================================================

ODBC_API = ctypes.windll.odbc32
ADDR = ctypes.byref

c_short = ctypes.c_short
buffer = memoryview

str_8b = bytes
from_buffer_u = lambda buffer: buffer.value
UCS_buf = lambda s: s
SQLExecute = ODBC_API.SQLExecute
class BinaryNullType(): pass
BinaryNull = BinaryNullType()
shared_env_h = None
pooling = True

BLANK_BYTE = str_8b()
SQLFetch = ODBC_API.SQLFetch
SQLGetData = ODBC_API.SQLGetData

def get_type(v):

    if isinstance(v, bool):
        return ('b',)
    elif isinstance(v, unicode):
        if len(v) >= 255:
            return  ('U',(len(v)//1000+1)*1000*2)
        else:
            return ('u',)
    elif isinstance(v, (str_8b,str)):
        if len(v) >= 255:
            return  ('S',(len(v)//1000+1)*1000)
        else:
            return  ('s',)
    elif isinstance(v, (int, long)):
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
        t = v.as_tuple()
        return  ('D',(len(t[1]),0 - t[2]))


    elif isinstance (v, datetime.datetime):
        return ('dt',)
    elif isinstance (v, datetime.date):
        return ('d',)
    elif isinstance(v, datetime.time):
        return ('t',)
    elif isinstance (v, (bytearray, buffer)):
        return ('bi',(len(v)//1000+1)*1000)

    return type(v)


py_ver = sys.version[:3]
py_v3 = py_ver >= '3.0'

def TupleRow(cursor):
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

    return Row

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



    def execute(self, query_string, params=None, many_mode=False, call_mode=False):
        if not self.connection:
            self.close()

        if params:

            if not isinstance(params, (tuple, list)):
                raise TypeError("Params must be in a list, tuple, or Row")


            if query_string != self.statement:
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

                elif param_types[col_num][0] == 'd':
                    if SQL_TYPE_DATE in self.connection.type_size_dic:
                        max_len = self.connection.type_size_dic[SQL_TYPE_DATE][0]
                    else:
                        max_len = 10
                    c_char_buf = param_val.isoformat()[:max_len]
                    if py_v3:
                        c_char_buf = bytes(c_char_buf,'ascii')
                    c_buf_len = len(c_char_buf)

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
                    param_buffer.value = c_char_buf

                if param_types[col_num][0] in ('U','u','S','s'):
                    param_buffer_len.value = SQL_NTS
                else:
                    param_buffer_len.value = c_buf_len

                col_num += 1
            ret = SQLExecute(self.stmt_h)
            if ret != SQL_SUCCESS:
                check_success(self, ret)

            if not many_mode:
                self._NumOfRows()
                self._UpdateDesc()
        else:
            self.execdirect(query_string)
            pass

        return self


    def fetchone(self):
        if not self.connection:
            print("closing")
            self.close()

        ret = SQLFetch(self.stmt_h)
        print(ret)
        if ret in (SQL_SUCCESS,SQL_SUCCESS_WITH_INFO):

            value_list = []
            col_num = 1
            for col_name, target_type, used_buf_len, ADDR_used_buf_len, alloc_buffer, ADDR_alloc_buffer, total_buf_len, buf_cvt_func, bind_data in self._ColBufferList:
                raw_data_parts = []
                while 1:
                    if bind_data:
                        ret = SQL_SUCCESS
                    else:
                        ret = SQLGetData(self.stmt_h, col_num, target_type, ADDR_alloc_buffer, total_buf_len, ADDR_used_buf_len)
                    if ret == SQL_SUCCESS:
                        if used_buf_len.value == SQL_NULL_DATA:
                            value_list.append(None)
                        else:
                            if raw_data_parts == []:
                                if target_type == SQL_C_BINARY:
                                    value_list.append(buf_cvt_func(alloc_buffer.raw[:used_buf_len.value]))
                                elif target_type == SQL_C_WCHAR:
                                    if used_buf_len.value < total_buf_len:
                                        ctypes.memset(ctypes.addressof(alloc_buffer) + used_buf_len.value, 0, 1)
                                    value_list.append(buf_cvt_func(from_buffer_u(alloc_buffer)))
                                elif alloc_buffer.value == '':
                                    value_list.append('')
                                else:
                                    value_list.append(buf_cvt_func(alloc_buffer.value))
                            else:
                                if target_type == SQL_C_BINARY:
                                    raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
                                elif target_type == SQL_C_WCHAR:
                                    raw_data_parts.append(from_buffer_u(alloc_buffer))
                                else:
                                    raw_data_parts.append(alloc_buffer.value)
                        break

                    elif ret == SQL_SUCCESS_WITH_INFO:
                        if target_type == SQL_C_BINARY:
                            raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
                        elif target_type == SQL_C_WCHAR:
                            raw_data_parts.append(from_buffer_u(alloc_buffer))
                        else:
                            raw_data_parts.append(alloc_buffer.value)

                    elif ret == SQL_NO_DATA:
                        break
                    else:
                        check_success(self, ret)

                if raw_data_parts != []:
                    if py_v3:
                        if target_type != SQL_C_BINARY:
                            data_parts = [x.decode("utf-8") if type(x) is bytes else x for x in raw_data_parts]
                            raw_value = ''.join(data_parts)
                        else:
                            raw_value = BLANK_BYTE.join(raw_data_parts)
                    else:
                        raw_value = ''.join(raw_data_parts)

                    value_list.append(buf_cvt_func(raw_value))
                col_num += 1
            return self._row_type(value_list)

        else:
            if ret == SQL_NO_DATA_FOUND:
                return None
            else:
                check_success(self, ret)

    def execdirect(self, query_string):
        if not self.connection:
            self.close()

        self._free_stmt()
        self._last_param_types = None
        self.statement = None
        if isinstance(query_string, unicode):
            c_query_string = wchar_pointer(UCS_buf(query_string))
            ret = ODBC_API.SQLExecDirectW(self.stmt_h, c_query_string, len(query_string))
        else:
            c_query_string = ctypes.c_char_p(query_string)
            ret = ODBC_API.SQLExecDirect(self.stmt_h, c_query_string, len(query_string))
        check_success(self, ret)
        self._NumOfRows()
        self._UpdateDesc()
        return self

    def _NumOfRows(self):
        if not self.connection:
            self.close()

        NOR = c_ssize_t()
        ret = SQLRowCount(self.stmt_h, ADDR(NOR))
        if ret != SQL_SUCCESS:
            check_success(self, ret)
        self.rowcount = NOR.value
        return self.rowcount

    def _UpdateDesc(self):
        if not self.connection:
            self.close()

        force_unicode = self.connection.unicode_results
        if force_unicode:
            Cname = create_buffer_u(1024)
        else:
            Cname = create_buffer(1024)

        Cname_ptr = c_short()
        Ctype_code = c_short()
        Csize = ctypes.c_size_t()
        Cdisp_size = c_ssize_t(0)
        CDecimalDigits = c_short()
        Cnull_ok = c_short()
        ColDescr = []
        self._ColTypeCodeList = []
        NOC = self._NumOfCols()
        for col in range(1, NOC+1):

            ret = ODBC_API.SQLColAttribute(self.stmt_h, col, SQL_DESC_DISPLAY_SIZE, ADDR(create_buffer(10)),
                10, ADDR(c_short()),ADDR(Cdisp_size))
            if ret != SQL_SUCCESS:
                check_success(self, ret)

            if force_unicode:

                ret = ODBC_API.SQLDescribeColW(self.stmt_h, col, Cname, len(Cname), ADDR(Cname_ptr),\
                    ADDR(Ctype_code),ADDR(Csize),ADDR(CDecimalDigits), ADDR(Cnull_ok))
                if ret != SQL_SUCCESS:
                    check_success(self, ret)
            else:

                ret = ODBC_API.SQLDescribeCol(self.stmt_h, col, Cname, len(Cname), ADDR(Cname_ptr),\
                    ADDR(Ctype_code),ADDR(Csize),ADDR(CDecimalDigits), ADDR(Cnull_ok))
                if ret != SQL_SUCCESS:
                    check_success(self, ret)

            col_name = from_buffer_u(Cname)
            if self.lowercase:
                col_name = col_name.lower()

            ColDescr.append((col_name, SQL_data_type_dict.get(Ctype_code.value,(Ctype_code.value,))[0],Cdisp_size.value,\
                Csize.value, Csize.value,CDecimalDigits.value,Cnull_ok.value == 1 and True or False))
            self._ColTypeCodeList.append(Ctype_code.value)

        if len(ColDescr) > 0:
            self.description = ColDescr
            self._row_type = self.row_type_callable(self)
        else:
            self.description = None
        self._CreateColBuf()

    def _NumOfCols(self):
        if not self.connection:
            self.close()

        NOC = c_short()
        ret = SQLNumResultCols(self.stmt_h, ADDR(NOC))
        if ret != SQL_SUCCESS:
            check_success(self, ret)
        return NOC.value

    def _CreateColBuf(self):
        if not self.connection:
            self.close()
        self._free_stmt(SQL_UNBIND)
        NOC = self._NumOfCols()
        self._ColBufferList = []
        bind_data = True
        for col_num in range(NOC):
            col_name = self.description[col_num][0]
            col_size = self.description[col_num][2]
            col_sql_data_type = self._ColTypeCodeList[col_num]

            target_type = SQL_data_type_dict[col_sql_data_type][2]
            dynamic_length = SQL_data_type_dict[col_sql_data_type][5]
            total_buf_len = SQL_data_type_dict[col_sql_data_type][4]

            if total_buf_len > 20500:
                total_buf_len = self._outputsize.get(None,total_buf_len)
            total_buf_len = self._outputsize.get(col_num, total_buf_len)

            if col_size >= 1024:
                dynamic_length = True

            alloc_buffer = SQL_data_type_dict[col_sql_data_type][3](total_buf_len)

            used_buf_len = c_ssize_t()

            force_unicode = self.connection.unicode_results

            if force_unicode and col_sql_data_type in (SQL_CHAR,SQL_VARCHAR,SQL_LONGVARCHAR):
                target_type = SQL_C_WCHAR
                alloc_buffer = create_buffer_u(total_buf_len)

            buf_cvt_func = self.connection.output_converter[self._ColTypeCodeList[col_num]]

            if bind_data:
                if dynamic_length:
                    bind_data = False
            self._ColBufferList.append([col_name, target_type, used_buf_len, ADDR(used_buf_len), alloc_buffer, ADDR(alloc_buffer), total_buf_len, buf_cvt_func, bind_data])

            if bind_data:
                ret = ODBC_API.SQLBindCol(self.stmt_h, col_num + 1, target_type, ADDR(alloc_buffer), total_buf_len, ADDR(used_buf_len))
                if ret != SQL_SUCCESS:
                    check_success(self, ret)

    def _free_stmt(self, free_type = None):
        if not self.connection:
            self.close()

        if not self.connection.connected:
            raise ProgrammingError('HY000','Attempt to use a closed connection.')

        if free_type in (SQL_CLOSE, None):
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_CLOSE)
            if ret != SQL_SUCCESS:
                check_success(self, ret)
        if free_type in (SQL_UNBIND, None):
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_UNBIND)
            if ret != SQL_SUCCESS:
                check_success(self, ret)
        if free_type in (SQL_RESET_PARAMS, None):
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_RESET_PARAMS)
            if ret != SQL_SUCCESS:
                check_success(self, ret)


#================================================================

class Connection:
    def __init__(self, connectString = '', autocommit = False, ansi = False, timeout = 0, unicode_results = use_unicode, readonly = False, **kargs):
        self.connected = 0
        self.type_size_dic = {}
        self.ansi = False
        self.unicode_results = False
        self.dbc_h = ctypes.c_void_p()
        self._autocommit = None
        self.readonly = False
        self.timeout = 0
        # self._cursors = []
        for key, value in list(kargs.items()):
            if value is not None:
                connectString = connectString + key + '=' + value + ';'
        self.connectString = connectString


        self.clear_output_converters()

        try:
            lock.acquire()
            if shared_env_h is None:
                AllocateEnv()
        finally:
            lock.release()


        ret = ODBC_API.SQLAllocHandle(SQL_HANDLE_DBC, shared_env_h, ADDR(self.dbc_h))
        print(SQL_HANDLE_DBC, shared_env_h, ADDR(self.dbc_h),"00init")
        check_success(self, ret)

        self.connection_timeout = connection_timeout


        self.connect(connectString, autocommit, ansi, timeout, unicode_results, readonly)


    def connect(self, connectString = '', autocommit = False, ansi = False, timeout = 0, unicode_results = use_unicode, readonly = False):

        if timeout != 0:
            ret = ODBC_API.SQLSetConnectAttr(self.dbc_h, SQL_ATTR_LOGIN_TIMEOUT, timeout, SQL_IS_UINTEGER)
            check_success(self, ret)


        self.ansi = ansi
        if not ansi:
            c_connectString = wchar_pointer(UCS_buf(self.connectString))
            odbc_func = ODBC_API.SQLDriverConnectW
        else:
            c_connectString = ctypes.c_char_p(self.connectString)
            odbc_func = ODBC_API.SQLDriverConnect

        if ODBC_API._name != 'odbc32':
            try:
                lock.acquire()
                ret = odbc_func(self.dbc_h, 0, c_connectString, len(self.connectString), None, 0, None, SQL_DRIVER_NOPROMPT)
            finally:
                lock.release()
        else:
            ret = odbc_func(self.dbc_h, 0, c_connectString, len(self.connectString), None, 0, None, SQL_DRIVER_NOPROMPT)

        print("001",self.connectString, ret)
        print(check_success(self, ret))
        print("002")

        self.autocommit = autocommit

        self.readonly = readonly

        ret = ODBC_API.SQLSetConnectAttr(self.dbc_h, SQL_ATTR_ACCESS_MODE, self.readonly and SQL_MODE_READ_ONLY or SQL_MODE_READ_WRITE, SQL_IS_UINTEGER)
        check_success(self, ret)

        self.unicode_results = unicode_results
        self.connected = 1


    def clear_output_converters(self):
        self.output_converter = {}
        for sqltype, profile in SQL_data_type_dict.items():
            self.output_converter[sqltype] = profile[1]


    def cursor(self, row_type_callable=None, lowercase=True):
        if not self.connected:
            raise ProgrammingError('HY000','Attempt to use a closed connection.')
        cur = Cursor(self, row_type_callable=row_type_callable, lowercase=lowercase)
        return cur


    def rollback(self):
        if not self.connected:
            raise ProgrammingError('HY000','Attempt to use a closed connection.')
        ret = SQLEndTran(SQL_HANDLE_DBC, self.dbc_h, SQL_ROLLBACK)
        if ret != SQL_SUCCESS:
            check_success(self, ret)


    def close(self):
        if not self.connected:
            raise ProgrammingError('HY000','Attempt to close a closed connection.')


        if self.connected:
            if not self.autocommit:
                self.rollback()
            ret = ODBC_API.SQLDisconnect(self.dbc_h)
            check_success(self, ret)
        ret = ODBC_API.SQLFreeHandle(SQL_HANDLE_DBC, self.dbc_h)
        check_success(self, ret)
        self.connected = 0


def AllocateEnv():
    if pooling:
        ret = ODBC_API.SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, SQL_CP_ONE_PER_HENV, SQL_IS_UINTEGER)
        check_success(SQL_NULL_HANDLE, ret)

    global shared_env_h
    shared_env_h  = ctypes.c_void_p()
    ret = ODBC_API.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, ADDR(shared_env_h))
    check_success(shared_env_h, ret)

    ret = ODBC_API.SQLSetEnvAttr(shared_env_h, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0)
    check_success(shared_env_h, ret)


def check_success(ODBC_obj, ret):
    if ret not in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA):
        if isinstance(ODBC_obj, Cursor):
            ctrl_err(SQL_HANDLE_STMT, ODBC_obj.stmt_h, ret, ODBC_obj.ansi)
        elif isinstance(ODBC_obj, Connection):
            ctrl_err(SQL_HANDLE_DBC, ODBC_obj.dbc_h, ret, ODBC_obj.ansi)
        else:
            ctrl_err(SQL_HANDLE_ENV, ODBC_obj, ret, False)



conn_str = (
    "DRIVER={Devart ODBC Driver for PostgreSQL};"
    "DATABASE=[DB]};"
    "UID=postgres;"
    "PWD=[parola];"
    "SERVER=localhost;"
    "PORT=[port];"
    )

conn = Connection(conn_str)
curs = conn.cursor().execute('SELECT * FROM "admin";')
print(curs.fetchone())
conn.close()