
import pyrfc
import re

QE2HOST   = '10.127.2.52' ;
QE2CLIENT = '100' ;
QE2SYSNR  = '00' ;
QE2USER   = 'WSAPEW12' ;
QE2PASSWD = 'Abcd1234' ;
QE2SAPROUTER = '/H/10.127.42.28/S/3299' ;


QB0HOST   = '10.127.2.20' ;
QB0CLIENT = '100' ;
QB0SYSNR  = '00' ;
QB0USER   = 'WSAPEW12' ;
QB0PASSWD = 'Abcd1234' ;
QB0SAPROUTER = '/H/10.127.42.28/S/3299' ;

class main():
    def __init__(self, ASHOST, SYSNR, CLIENT, USER, PASSWD, SAPROUTER):
        self.conn = pyrfc.Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT,
                                     user=USER, passwd=PASSWD, saprouter=SAPROUTER);

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

        data = [];

        (Fields, SQLTable, Where) = self.select_parse(sqlstr) ;

        if Fields[0] == '*':
            Fields = '' ;
        else:
            Fields = [{'FIELDNAME': x} for x in Fields] ;  # Notice the format

        options = [{'TEXT': Where}] ;

        rowcount = MaxRows ;

        tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS=Fields, \
                                OPTIONS=options, ROWCOUNT=MaxRows, ROWSKIPS=FromRow) ;
        for item in tables["DATA"] :
            data.append(item['WA']);
        return len(data), data;


#global variable
maxrows = 100000000
fromrow = 0

# filter  --------------------------------------------------------------------------------------------------

# bundle
bundle_filter_qe2 = 'where ACTIVE = \'A\' and DELETED <> \'X\' '  ;
bundle_filter_qb0 = 'where ACTIVE = \'A\' '  ;

# claim
claim_filter_qe2 = 'where ACTIVE = \'A\' and DELETED <> \'X\' ' + \
                   'and XDUMMYCLAIM = \'\' ' ;
claim_filter_qb0 = 'where ACTIVE = \'A\' and XDUMMYCLAIM = \'\' ' ;

# claim item
claimItem_filter_qe2 = 'where ACTIVE = \'A\' and DELETED <> \'X\' ' ;
claimItem_filter_qb0 = 'where ACTIVE = \'A\'' ;

# payment
payment_filter_qe2 = 'where ACTIVE = \'A\' and DELETED <> \'X\' ' ;
payment_filter_qb0 = 'where ACTIVE = \'A\'' ;

# claima
claima_filter_qe2 = 'where ACTIVE = \'A\' and DELETED <> \'X\' and SUBOBJCAT = \'C\' ' ;
claima_filter_qb0 = 'where ACTIVE = \'A\'' ;

# QE2  -----------------------------------------------------------------------------------------------------

QE2CONN = main(ASHOST = QE2HOST ,
               SYSNR  = QE2SYSNR,
               CLIENT = QE2CLIENT ,
               USER   = QE2USER,
               PASSWD = QE2PASSWD,
               SAPROUTER = QE2SAPROUTER ) ;

# bundle
(bundle_count_qe2, bundle_data_qe2)  = QE2CONN.query(u'select CCEVENT from ICLCCEVENT ' +
                                                      bundle_filter_qe2, maxrows, fromrow)
print('qe2 bundle count:', bundle_count_qe2);

# claim
(claim_count_qe2, claim_data_qe2)  = QE2CONN.query(u'select claim from iclclaim ' +
                                                   claim_filter_qe2, maxrows, fromrow)
print('qe2 claim count:', claim_count_qe2);

(claimItem_count_qe2, claimItem_data_qe2)  = QE2CONN.query(u'select CLAIM, SUBCLAIM, ITEM from ICLITEM ' +
                                                           claimItem_filter_qe2, maxrows, fromrow)
print('qe2 claim item count:', claimItem_count_qe2);

# payment
(payment_count_qe2, payment_data_qe2) = QE2CONN.query(u'select CLAIM, SUBCLAIM, PAYMENT, PAYMENTITEM from V_ICLPAYMENT' +
                                                      payment_filter_qe2, maxrows, fromrow)
print('qe2 payment count:', payment_count_qe2);

# claima
(claima_count_qe2, claima_data_qe2) = QE2CONN.query(u'select CLAIM, SUBOBJCAT, SUBOBJECT, OBJCAT, CLOBJECT from ICLCLAIMA' +
                                                    claima_filter_qe2, maxrows, fromrow);

print('qe2 claima count:', claima_count_qe2);

# QB2  -----------------------------------------------------------------------------------------------------

QB0CONN = main(ASHOST = QB0HOST ,
               SYSNR  = QB0SYSNR,
               CLIENT = QB0CLIENT ,
               USER   = QB0USER,
               PASSWD = QB0PASSWD,
               SAPROUTER = QB0SAPROUTER ) ;

# bundle
(bundle_count_qb0, bundle_data_qb0) = QB0CONN.query(u'select CCEVENT from /BIC/AZSD00482 ' +
                                                    bundle_filter_qb0, maxrows, fromrow)
print('qb0 bundle count:', bundle_count_qb0);

# claim
(claim_count_qb0, claim_data_qb0)  = QB0CONN.query(u'select claim from /BIC/AZSD00472 ' +
                                                   claim_filter_qb0, maxrows, fromrow)
print('qb0 claim count:', claim_count_qb0);

# claim item
(claimItem_count_qb0, claimItem_data_qb0)  = QB0CONN.query(u'select CLAIM, SUBCLAIM, ITEM from /BIC/AZSD00452 ' +
                                                          claimItem_filter_qb0, maxrows, fromrow)
print('qb0 claim item count:', claimItem_count_qb0);

# payment
(payment_count_qb0, payment_data_qb0) = QB0CONN.query(u'select CLAIM, SUBCLAIM, PAYMENT, PAYMENTITEM from /BIC/AZSD00432 ' +
                                                      payment_filter_qb0, maxrows, fromrow)
print('qb0 payment count:', payment_count_qb0);

# claima
(claima_count_qb0, claima_data_qb0) = QB0CONN.query(u'select CLAIM, SUBOBJCAT, SUBOBJECT, OBJCAT, CLOBJECT from /BIC/AZSD00462 ' +
                                                    claima_filter_qb0, maxrows, fromrow);
'''
claima_data_qb0  = list(set(claima_data_qb0).intersection(set(claim_data_qb0)));
claima_count_qb0 = len(claima_data_qb0) ;
'''
print('qb0 claima count:', claima_count_qb0);

# result  --------------------------------------------------------------------------------------------------

# bundle
print('bundle difference:', list(set(bundle_data_qe2).difference(set(bundle_data_qb0))));

# claim
print('claim difference:', list(set(claim_data_qe2).difference(set(claim_data_qb0))));

# claim item
print('claim item difference:', list(set(claimItem_data_qe2).difference(set(claimItem_data_qb0))));

# payment
print('payment difference:', list(set(payment_data_qe2).difference(set(payment_data_qb0))));

# claima
print('claima difference:', list(set(claima_data_qe2).difference(set(claima_data_qb0))));
