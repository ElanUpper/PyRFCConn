
import pyrfc
import re

ASHOST = '10.58.5.229'
CLIENT = '200'
SYSNR  = '01'
USER   = 'i321482'
PASSWD = 'Abcd1234'

class main():
    def __init__(self):
        self.conn = pyrfc.Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD);

    def select_parse(self, statement):
        statement = " ".join([x.strip('\t') for x in statement.upper().split('\n')])

        if 'WHERE' not in statement:
            statement = statement + ' WHERE '

        regex = re.compile("SELECT(.*)FROM(.*)WHERE(.*)")

        parts = regex.findall(statement)
        parts = parts[0]
        fields = [x.strip() for x in parts[0].split(',')]
        tabname = parts[1].strip()
        where = parts[2].strip()

        cleaned = [fields, tabname, where]
        return cleaned

    def query(self, sqlstr = '', MaxRows=50, FromRow=0):

        (Fields, SQLTable, Where) = self.select_parse(sqlstr) ;

        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME': x} for x in Fields]  # Notice the format

        options = [{'TEXT': Where}]

        rowcount = MaxRows

        tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS=Fields, \
                                OPTIONS=options, ROWCOUNT=MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []

        data_fields = tables["DATA"]  # pull the data part of the result set
        data_names = tables["FIELDS"]  # pull the field name part of the result set

        headers = [x['FIELDNAME'] for x in data_names]  # headers extraction
        long_fields = len(data_fields)  # data extraction
        long_names = len(data_names)  # full headers extraction if you want it

        # now parse the data fields into a list
        for line in range(0, long_fields):
            fields.append(data_fields[line]["WA"].strip())

        # for each line, split the list by the '|' separator
        fields = [x.strip().split('|') for x in fields]

        # return the 2D list and the headers
        return fields, headers


# Init the class and connect
# I find this can be very slow to do...
s = main()

# Choose your fields and table
fields = ['MATNR', 'EAN11']
table = 'MEAN'
where = 'MATNR <> 0'
maxrows = 10
fromrow = 0

# query SAP
(results, headers) = s.query(u'select matnr, ean11 from mean where matnr <> 0', maxrows, fromrow)
print (headers)
print (results)