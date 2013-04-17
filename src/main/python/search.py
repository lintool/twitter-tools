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
    parser.add_argument('-host', '--host', dest="host", help='server to connect to', required=True)
    parser.add_argument('-port', '--port', type=int, dest="port", help='port to use', required=True)
    parser.add_argument('-file', '--file', dest="query_file", help='XML file containing queries', required=True)
    args = parser.parse_args()

    # Init thrift connection and protocol handlers
    transport = TSocket.TSocket(args.host, args.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = TrecSearch.Client(protocol)

    # Open the connection to the server
    transport.open()

    # Parse the query file
    tree = ET.parse(args.query_file)
    parameters = tree.getroot()

    # Read the query file and run a search for each of the queries
    for query in parameters:
        q = ttypes.TQuery()
        q.text = query.find('text').text.strip()
        q.name = query.find('number').text.strip()
        q.max_id = long(query.find('lastrel').text.strip())
        q.num_results = 1000

        # Performs the actual search on index
        results = client.search(q)

        i = 1
        for result in results:
            # TREC_eval formatted line
            print "%s Q0 %d %d %f" % (q.name, result.id, i, result.rsv)
            i += 1

    # Close connection
    transport.close()

except Thrift.TException, tx:
    print 'Something went wrong : %s' % (tx.message)
