
import pyrfc

'''

important urls:

http://www.alexbaker.me/code/python-and-sap-part-1-connecting-to-sap
http://sap.github.io/PyRFC/client.html
https://github.com/SAP/PyRFC

'''

ASHOST    = '10.58.5.229'
CLIENT    = '200'
SYSNR     = '01'
USER      = 'i321482'
PASSWD    = 'Abcd1234'
SAPROUTER = ''

with pyrfc.Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, \
                        user=USER, passwd=PASSWD, saprouter=SAPROUTER) as conn:
    result = conn.call('STFC_CONNECTION', REQUTEXT=u'Hello SAP!') ;
    print(result)