import sys

"""
A demonstration of how to use the python thrift bindings to retrieve tweets from the TREC 2013 API. 

This script requires the python-thrift package, which can installed using 'pip install thrift'.

To execute this script:
    python TrecSearchThriftClientCli.py -host='host' -port=port -group='team_name' -token='access_token' -qid='MB01' -q='BBC World Service staff cuts' -runtag='lucene4lm' -max_id=34952194402811905

"""

from Search import TrecSearch, ttypes

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import argparse

try:
    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', dest="host", help='server to connect to', required=True)
    parser.add_argument('-port',type=int, dest="port", help='port to use', required=True)
    parser.add_argument('-group', dest="group", help='group id', required=True)
    parser.add_argument('-token', dest="token", help='access token', required=True)
    parser.add_argument('-qid', dest="qid", help='query id', required=False, default='MB01')
    parser.add_argument('-q', dest="query", help='query text', required=False, default='BBC World Service staff cuts')
    parser.add_argument('-runtag', dest="run_tag", help='runtag', required=False, default='lucene4lm')
    parser.add_argument('-max_id', dest="max_id", help='maxid', required=False, default=34952194402811905)
    parser.add_argument('-num_results', dest="num_results", help='number of results', required=False, default=10)
    args = parser.parse_args()

    # Init thrift connection and protocol handlers
    transport = TSocket.TSocket(args.host, args.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = TrecSearch.Client(protocol)

    # Open the connection to the server
    transport.open()

    # Create a new query
    q = ttypes.TQuery()
    q.group = args.group
    q.token = args.token
    q.text = args.query
    q.max_id = long(args.max_id)
    q.num_results = int(args.num_results)

    # Performs the actual search
    results = client.search(q)

    for i, result in enumerate(results, 1):
        # TREC_eval formatted line
        print "%s Q0 %d %d %f %s" % (args.qid, result.id, i, result.rsv, args.run_tag)

    # Close connection
    transport.close()

except Thrift.TException, tx:
    print 'Thrift TException: %s' % (tx.message)
