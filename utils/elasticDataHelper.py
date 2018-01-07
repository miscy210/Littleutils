# -*- coding: utf8 -*-

from elasticsearch import Elasticsearch
from elasticsearch import helpers
try:
    import urllib.request as urllib
    import urllib.parse as urlparse
except:
    import urllib
    import urlparse
import time
import json
import traceback
import argparse


class ESDump():
    def __init__(self, es_host, index_name, doc_type, dump_type="data", fields=[]):
        self.es_host = es_host
        self.index_name = index_name
        self.doc_type = doc_type
        self.dump_type = dump_type
        self.fields = fields
        self.es_url = parse_es_url(self.es_host, self.index_name, self.doc_type)

    def handle(self):
        if self.dump_type == "mapping":
            return self.dump_mapping()
        else:
            if self.fields:
                return self.dump_data_with_fields()
            else:
                return self.dump_data_all()

    def dump_mapping(self):
        es_mappings_url = self.es_url + "/_mapping"
        mapping_res = urllib.urlopen(es_mappings_url).read().decode('utf-8')
        mapping_res = clean_mapping(mapping_res)
        return mapping_res

    def dump_data(self, query):
        if self.fields:
            opt = "update"
        else:
            opt = "index"
        es_host = urlparse.urlparse(self.es_url).netloc
        client = Elasticsearch(es_host)
        scroll = '1m'
        result = helpers.scan(client,
                              query=query,
                              index=self.index_name,
                              doc_type=self.doc_type,
                              scroll=scroll,
                              request_timeout=30,
                              size=20)
        doc = []
        startime = time.time()
        count = 1
        for hit in result:
            optinfo = {opt: {"_index": hit['_index'], "_type": hit['_type'], "_id": hit['_id']}}
            doc.append(json.dumps(optinfo))
            if opt == "index":
                docinfo = hit['_source']
            else:
                docinfo = {"doc": hit['_source']}
            doc.append(json.dumps(docinfo))
            if len(doc) >= 2000:
                yield doc
                print("this %d documents update cost %s" % (count, time.time() - startime))
                doc = []
            count += 1
        if doc:
            yield doc
            print("this %d documents update cost %s" % (count, time.time() - startime))

    def dump_data_all(self):
        query = {"query": {"match_all": {}}}
        return self.dump_data(query)

    def dump_data_with_fields(self):
        query = {"query": {"match_all": {}}, "_source": self.fields}
        return self.dump_data(query)

    def writetofile(self, f_handle, doc):
        if isinstance(doc, list):
            f_handle.write("\n".join(doc))
        else:
            f_handle.write(doc)
        f_handle.write("\n")

    def writetoes(self, es_handle, doc):
        es_handle.handle(doc, self.dump_type)


class ES_loads():
    def __init__(self, es_host, index_name, doc_type, dump_type="data"):
        self.es_url = parse_es_url(es_host, index_name, doc_type)
        self.es_host = urlparse.urlparse(self.es_url).netloc
        self.index_name = index_name
        self.doc_type = doc_type
        self.dump_type = dump_type
        self.cache = []
        self.client = Elasticsearch(self.es_host)

    def handle(self, doc, loads_type, rename_type=None):
        if loads_type == "mapping":
            self.handle_mapping(doc, rename_type)
        else:
            if isinstance(doc, list):
                self.handle_data_docs(doc, rename_type)
            else:
                self.handle_data(doc, rename_type)

    def handle_mapping(self, doc, rename_type):
        if not rename_type:
            rename_type = self.doc_type
        _, mapping = json.loads(doc).popitem()
        if self.client.indices.exists(self.index_name):
            if rename_type not in mapping['mappings']:
                raise KeyError("Specify the type name to be renamed, because we cant find the typename in mappings.")
            self.client.indices.put_mapping(index=self.index_name, doc_type=self.doc_type,
                                            body=mapping['mappings'][rename_type])
        else:
            this_mapping = {"mappings": {self.doc_type: mapping["mappings"][rename_type]}}
            self.client.indices.create(index=self.index_name, body=this_mapping)

    def handle_data_docs(self, docs, rename_type):
        for doc in docs:
            self.handle_data(doc, rename_type)

    def handle_data(self, doc, rename_type):
        if isinstance(doc, str):
            jdoc = json.loads(doc)
        else:
            jdoc = doc
        if "index" in jdoc:
            opt = "index"
        elif "update" in jdoc:
            opt = "update"
        else:
            opt = None
        if opt:
            jdoc[opt]['_index'] = self.index_name
            jdoc[opt]['_type'] = self.doc_type
        self.cache.append(jdoc)
        try:
            if len(self.cache) >= 1000:
                self.client.bulk(self.cache)
                print("500 done!")
                self.cache = []
        except:
            traceback.print_exc()

    def _flush(self):
        if self.cache:
            self.client.bulk(self.cache)


def clean_mapping(mapping):
    return mapping.replace(', "analyzer": "complex"', '').replace('"analyzer": "complex", ',
                                                                  '').replace(
        ', "fielddata":false', '').replace('"fielddata": false, ', '').replace(',"analyzer": "complex"',
                                                                               '').replace(
        '"analyzer": "complex",', '').replace(',"fielddata":false', '').replace('"fielddata": false,', '')


def parse_es_url(es_host, index_name, doc_type, port=9200):
    """暂时不能处理无端口的es host错误的"""
    port = str(port)
    es_host = es_host.strip("/")
    urlinfo = urlparse.urlparse(es_host)
    protocol = urlinfo.scheme
    if protocol:
        this_port = str(urlinfo.port)
        if port:
            if not this_port:
                es_host = ":".join([es_host, str(port)])
            elif port != this_port:
                es_host = es_host.replace(this_port, port)
        try:
            urllib.urlopen(es_host)
        except urllib.error.URLError:
            other_protocol = "https" if protocol == "http" else "http"
            es_host = es_host.replace(protocol, other_protocol)
            return parse_es_url(es_host, index_name, doc_type)
        except:
            traceback.print_exc()
    else:
        es_host = "://".join(["http", es_host])
        return parse_es_url(es_host, index_name, doc_type)
    return "/".join([es_host, index_name, doc_type])


def main():
    usage = "python dumpscript.py -i <input source> -o <outpur target> -t <source type>"
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action="store", dest="source")
    parser.add_argument("-o", "--output", action="store", dest="target")
    parser.add_argument("-t", "--type", action="store", dest="dump_type")
    parser.add_argument("-f", "--fields", type=list, nargs='+', dest="fields")
    parser.add_argument("-r", "--rename_type", action="store", dest="rename_type")
    source = parser.parse_args().source
    target = parser.parse_args().target
    dump_type = parser.parse_args().dump_type
    fields = [] if not parser.parse_args().fields else ["".join(item) for item in parser.parse_args().fields]
    rename_type = parser.parse_args().rename_type
    if not fields:
        fields = []
    if not rename_type:
        rename_type = None
    if source.startswith("http"):
        s_urlinfo = source.split("/")
        s_es_host = "/".join(s_urlinfo[:-2])
        s_index_name = s_urlinfo[-2]
        s_doc_type = s_urlinfo[-1]
        adump = ESDump(es_host=s_es_host, index_name=s_index_name, doc_type=s_doc_type, dump_type=dump_type,fields=fields)
        res = adump.handle()

        if target.startswith("http"):
            t_urlinfo = target.split("/")
            t_es_host = "/".join(t_urlinfo[:-2])
            t_index_name = t_urlinfo[-2]
            t_doc_type = t_urlinfo[-1]
            aload = ES_loads(es_host=t_es_host,index_name=t_index_name,doc_type=t_doc_type,dump_type=dump_type)
            if dump_type == "mapping":
                aload.handle(res,dump_type,rename_type=rename_type)
            else:
                for line in res:
                    aload.handle(line, dump_type, rename_type=rename_type)
                aload._flush()
        else:
            f = open(target,'w')
            if dump_type == "mapping":
                adump.writetofile(f,res)
            else:
                for line in res:
                    adump.writetofile(f, line)
            f.close()
    else:
        in_f = open(source,'r')
        if target.startswith("http"):
            t_urlinfo = target.split("/")
            t_es_host = "/".join(t_urlinfo[:-2])
            t_index_name = t_urlinfo[-2]
            t_doc_type = t_urlinfo[-1]
            aload = ES_loads(es_host=t_es_host, index_name=t_index_name, doc_type=t_doc_type, dump_type=dump_type)
            if dump_type == 'mapping':
                aload.handle(in_f.readlines()[0].strip(), dump_type, rename_type=rename_type)
            else:
                for line in in_f.readlines():
                    aload.handle(line.strip(), dump_type, rename_type=rename_type)
                aload._flush()
        else:
            out_f = open(target,'w')
            out_f.write(in_f.read)
            out_f.close()
        in_f.close()

if __name__ == "__main__":
    main()
