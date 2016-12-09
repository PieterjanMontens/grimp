import pysolr
import logging

logger = logging.getLogger('grimplogger')
class SolrImporter:
    _url = 'http://localhost:8080/solr/adv_core/'
    _sorl = None
    _cfg = None

    def __init__(self,cfg):
        self._cfg = cfg
        self._url = cfg['url']
        return None

    #################### API
    ########################
    def imports(self,data):
        Node = self._mk_node(data)
        logger.debug('Solr importer ready to import node {0}'.format(Node))
        Solr = pysolr.Solr(self._url, timeout=10)
        Solr.add([Node])
        try:
            del Solr
            logger.info('Document imported')
            return None
        except ReferenceError:
            logger.error('Reference error, not imported')
            return None

    def test_enable(self):
        return None

    ############### INTERNAL
    ########################
    def _mk_node(self,data):
        Base = {}
        for inkey, outkey in self._cfg['in2out']:
            Base[outkey] = data[inkey]

        ### Languages
        #if data["french"] == True:
        #    Base["language"].append("fr")
        #if data["dutch"]  == True:
        #    Base["language"].append("nl")

        ### Mentions
 vis        #others = data["other_advices"]
        #current = data["number"]
        #if current in others:
        #    others.remove(current)
        #if isinstance(others,list):
        #    Base["mentions"] = others

        ### Types
        #if data["doc_type_fr"] is not "not_found":
        #    Base["type"].append(data["doc_type_fr"])
        #if data["doc_type_nl"] is not "not_found":
        #    Base["type"].append(data["doc_type_nl"])

        ## Persons
        #Base["person"] = data["persons"]

        return Base

    def __format_date(self,date):
        return date[:10] + "T" + date[11:] + "Z"


