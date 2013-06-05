import sys

# Path the the automatically generated thrift files
# Alternativly, the files in the /src/gen/python directory can be added to your python path
sys.path.append('./gen/python')

from Search import TrecSearch, ttypes

import argparse
import xml.etree.ElementTree as ET

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', dest="host", help='server to connect to', required=True)
    parser.add_argument('-port',type=int, dest="port", help='port to use', required=True)
    parser.add_argument('-qid', dest="qid", help='query id', required=True)
    parser.add_argument('-q', dest="query", help='query text', required=True)
    parser.add_argument('-runtag', dest="run_tag", help='runtag', required=True)
    parser.add_argument('-max_id', dest="max_id", help='maxid', required=True)
    parser.add_argument('-num_results', dest="num_results", help='number of results', required=True)
    parser.add_argument('-group', dest="group", help='group id', required=True)
    parser.add_argument('-token', dest="token", help='access token', required=True)
    args = parser.parse_args()


    # Init thrift connection and protocol handlers
    transport = TSocket.TSocket(args.host, args.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = TrecSearch.Client(protocol)

    # Open the connection to the server
    transport.open()

    q = ttypes.TQuery()
    q.group = args.group
    q.token = args.token
    q.text = args.query
    q.max_id = long(args.max_id)
    q.num_results = long(args.num_results)

    print args.max_id
    print int(args.max_id)

    # Performs the actual search on index
    results = client.search(q)

    i = 1
    for result in results:
        # TREC_eval formatted line
        print "%s Q0 %d %d %f %s" % (args.qid, result.id, i, result.rsv, args.run_tag)
        i += 1

    # Close connection
    transport.close()

except Thrift.TException, tx:
    print 'Something went wrong : %s' % (tx.message)