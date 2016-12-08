## Pieterjan Montens 2016
## https://github.com/PieterjanMontens
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client

class Importer:
    __gdb = None
    __test = False
    __import = True

    #################################### PUBLIC METHODS
    def __init__(self,base_url):
        self.__gdb = GraphDatabase(base_url, username="neo4j", password="cactus")
        return None


    def imports(self,data):
        current = data["number"]
        avis = self.__avis_create(data)

        others = data["other_advices"]
        if current in others:
            others.remove(current)

        #print(data["other_advices"])
        #print(others)

        # Relation: Other advices
        self.__chamber_create(current, data["chamber"])

        for other in others:
            self.__other_create(current,other)

        # Relation: Persons / Authors
        for person in data["persons"]:
            self.__person_create(current,person)

        # Relation: Types
        has_fr = data["french"] == True
        has_nl = data["dutch"]  == True

        if has_fr and data["doc_type_fr"] is not "not_found":
            self.__types_create(current,  data["doc_type_fr"], "fr")

        if has_nl and data["doc_type_nl"] is not "not_found":
            self.__types_create(current,  data["doc_type_nl"], "nl")

        return None

    def test_enable(self):
        self.__test = True
        return self

    def import_disable(self):
        self.__import = False
        return self

    #################################### PRIVATE METHODS
    def __avis_create(self,data):
        self.__log("Handling avis {0}".format(data['number'])),
        if data["deadline"] == 'not_found':
            Ddline = 'NO_DEADLINE'
        else:
            Ddline = 'DEADLINE_{0}'.format(data["deadline"])

        has_fr = data["french"] == True
        has_nl = data["dutch"]  == True

        if has_fr and has_nl:
            Ln_label = "French:Dutch"
        elif has_fr:
            Ln_label = "French"
        elif has_nl:
            Ln_label = "Dutch"


        q_mask = "MERGE (n:Avis:{4}:{5} {{ number : '{0}', length: '{1}', date: '{2}', pages: '{3}'}}) RETURN n"
        q = q_mask.format(data['number'], data['length'],data['date'],data['pages']
                         ,Ddline,Ln_label)

        self.__querylog(q)
        result = self.__gdb.query(q=q, returns=(client.Node))[0][0]
        return result

    def __other_create(self,avis,other):
        self.__log("Creating relation to {0}".format(other))
        q_mask = "MERGE (n:Avis {{ number : '{1}'}}) WITH n MATCH (avis:Avis {{ number : '{0}'}}) MERGE (avis)-[r:MENTIONS]->(n)"
        q = q_mask.format(avis,other)
        self.__querylog(q)
        self.__gdb.query(q=q)

    def __person_create(self, avis, person):
        self.__log("Creating Person relation to {0}".format(person))
        q_mask = "MERGE (person:Person{{ name :'{1}'}})  WITH person MATCH (avis:Avis {{ number : '{0}'}}) MERGE (avis)-[:PERSONS]->(person)"
        q = q_mask.format(avis,person)
        self.__querylog(q)
        self.__gdb.query(q=q)

    def __deadline_create(self, avis, deadline):
        self.__log("Creating Deadline relation to {0}".format(deadline))
        q_mask = "MERGE (deadline:Deadline{{ type :'{1}'}}) WITH deadline MATCH (avis:Avis {{ number : '{0}'}}) MERGE (avis)-[:DEADLINE]->(deadline)"
        q = q_mask.format(avis,deadline)
        self.__querylog(q)
        self.__gdb.query(q=q)

    def __chamber_create(self, avis, chamber):
        self.__log(" Creating Chamber relation to {0}".format(chamber))
        q_mask = "MERGE (chamber:Chamber{{ name :'{1}'}}) WITH chamber MATCH (avis:Avis {{ number : '{0}'}})  MERGE (avis)-[:CHAMBER]->(chamber)"
        q = q_mask.format(avis,chamber)
        self.__querylog(q)
        self.__gdb.query(q=q)

    def __types_create(self, avis, typ, lang):
        self.__log("Creating {0} type relation to {1}".format(lang,typ))
        if lang is "fr":
            q_mask = "MERGE (type:Type:French{{ name :'{1}'}}) WITH type MATCH (avis:Avis {{ number : '{0}'}})  MERGE (avis)-[:TYPE]->(type)"
        elif lang is "nl":
            q_mask = "MERGE (type:Type:French{{ name :'{1}'}}) WITH type MATCH (avis:Avis {{ number : '{0}'}})  MERGE (avis)-[:TYPE]->(type)"

        q = q_mask.format(avis,typ)
        self.__querylog(q)
        self.__gdb.query(q=q)

    def __querylog(self,q):
        if self.__test:
            print("\n######## Generated Query ########\n"+q+"\n#################################\n")
        return self

    def __log(self,msg):
        if self.__test:
            print("\n" + msg)
        return self
