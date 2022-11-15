from elasticsearch import Elasticsearch
import argparse
import time
import random
import sys
import datetime as DT
import string
import copy

def main():
    # Set a parser object
    parser = argparse.ArgumentParser()
    parser.add_argument("--es_address", nargs='+', help="The address of your cluster (no protocol or port)", required=True)
    args = parser.parse_args()
    #args.es_address
    #es= Elasticsearch("esnode-0.elasticsearch-api.elasticsearchrepl1.svc.cluster.local:9200", http_auth=None, verify_certs=True,  ssl_context= None,  timeout=1200)
    es= Elasticsearch(args.es_address, http_auth=None, verify_certs=True,  ssl_context= None,  timeout=1200)
    #For read last 7 days data and on every index.
    now = DT.datetime.now()
    fromDate  =  (now - DT.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
    for es_index in es.indices.get('*'):
        print("es index "+es_index)
        #using scroll API to search docs for given indice
        result = es.search(index=es_index, body={"query":{"range": {"@timestamp": {"gte":fromDate, "lte":"now/d"}}}}, size=100 , scroll="5m")
        #result = es.search(index=es_index, body={"query":{"match_all":{}}}, size=100 , scroll="5m")
        for doc in result['hits']['hits']:
            print ("DOC ID"+doc['_id'])#, doc['_source']
        old_scroll_id = result['_scroll_id']
        results = result['hits']['hits']
        print("Lenght of hits "+str(len(results)))
        try:
            counter = 0
            while len(results)>0:
                for i, r in enumerate(results):
                    print(i)
                    print r['_id']
                result = es.scroll(scroll_id=old_scroll_id,
                    scroll='5m'  # length of time to keep search context
                    )
                # check if there's a new scroll ID
                if old_scroll_id != result['_scroll_id']:
                    print("NEW SCROLL ID:", result['_scroll_id'])
                # keep track of pass scroll _id
                old_scroll_id = result['_scroll_id']
                results = result['hits']['hits']
                print("Scroll id "+old_scroll_id)
                print("Lenght of hits " +str(len(results)))
                print("Wait for 10 seconds")
                time.sleep(10)
        except Exception as ex:
            print("Exception thrown while querying %s : %s",es_index, ex)
        print("Picking next indes after 1 minute")
        time.sleep(60)
try:
    main()
except Exception as e:
    print("Got unexpected exception. probably a bug, please report it.")
    print("")
    print(e.message)
    print("")
    sys.exit(1)