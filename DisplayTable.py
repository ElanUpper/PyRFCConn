
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


    def qry(self, Fields, SQLTable, Where='', MaxRows=50, FromRow=0):
        """A function to query SAP with RFC_READ_TABLE"""

        # By default, if you send a blank value for fields, you get all of them
        # Therefore, we add a select all option, to better mimic SQL.
        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME': x} for x in Fields]  # Notice the format

        # the WHERE part of the query is called "options"
        options = [{'TEXT': Where}]  # again, notice the format

        # we set a maximum number of rows to return, because it's easy to do and
        # greatly speeds up testing queries.
        rowcount = MaxRows

        # Here is the call to SAP's RFC_READ_TABLE
        tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS=Fields, \
                                OPTIONS=options, ROWCOUNT=MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []

        data_fields = tables["DATA"]  # pull the data part of the result set

        data_names = tables["FIELDS"]  # pull the field name part of the result set
        headers = [x['FIELDNAME'] for x in data_names]  # headers extraction only get fieldname

        long_fields = len(data_fields)  # data extraction
        # now parse the data fields into a list
        for line in range(long_fields):
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
(results, headers) = s.qry(fields, table, where, maxrows, fromrow)

print (headers)
print (results)
