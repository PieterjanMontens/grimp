#   Solr Field Schema FYI:
#
#   <!-- AVIS specific field definitions -->
#   <field name="number" type="int" indexed="true" stored="true" />
#
#   <field name="chamber" type="text_general" indexed="true" stored="true" />
#   <field name="deadline" type="text_general" indexed="true" stored="true" />
#   <field name="pages" type="int" indexed="true" stored="true" />
#   <field name="length" type="int" indexed="true" stored="true" />
#
#   <field name="language" type="text_general" indexed="true" stored="true" multiValued="true"/>
#   <field name="type" type="text_general" indexed="true" stored="true" multiValued="true"/>
#   <field name="person" type="text_general" indexed="true" stored="true" multiValued="true" />
#   <field name="mentions" type="int" indexed="true" stored="true" multiValued="true" />
#
#   <fieldType name="text" class="solr.TextField" positionIncrementGap="100">
#      <analyzer type="index">
#        <tokenizer class="solr.StandardTokenizerFactory"/>
#        <filter class="solr.StopFilterFactory" ignoreCase="true" words="stopwords.txt" />
#        <!-- in this example, we will only use synonyms at query time
#        <filter class="solr.SynonymFilterFactory" synonyms="index_synonyms.txt" ignoreCase="true" expand="false"/>
#        -->
#        <filter class="solr.LowerCaseFilterFactory"/>
#      </analyzer>
#      <analyzer type="query">
#        <tokenizer class="solr.StandardTokenizerFactory"/>
#        <filter class="solr.StopFilterFactory" ignoreCase="true" words="stopwords.txt" />
#        <filter class="solr.SynonymFilterFactory" synonyms="synonyms.txt" ignoreCase="true" expand="true"/>
#        <filter class="solr.LowerCaseFilterFactory"/>
#      </analyzer>
#    </fieldType>
#
#    <!-- The format for this date field is of the form 1995-12-31T23:59:59Z -->
#    <fieldType name="date" class="solr.TrieDateField" precisionStep="6" positionIncrementGap="0"/>
#
#   <uniqueKey>number</uniqueKey>
import pysolr

class SolrImporter:
    __url = 'http://localhost:8080/solr/adv_core/'
    __sorl = None

    def __init(self):
        #self.__solr = pysolr.Solr(self.__url, timeout=10)
        return None

    def imports(self,data):
        Node = self.__mk_node(data)
        Solr = pysolr.Solr(self.__url, timeout=10)
        Solr.add([Node])
        try:
            del Solr
            return None
        except ReferenceError:
            return None

    def test_enable(self):
        return None

    def __mk_node(self,data):
        Base = {"id"        : data["number"]
               ,"number"    : data["number"]
               ,"chamber"   : data["chamber"]
               ,"deadline"  : data["deadline"]
               ,"pages"     : data["pages"]
               ,"length"    : data["length"]
               ,"language"  : []
               ,"type"      : []
               ,"person"    : []
               ,"mentions"  : []
               ,"date"      : self.__format_date(data["date"])
               ,"text"      : data["text"]
               }

        ## Languages
        if data["french"] == True:
            Base["language"].append("fr")
        if data["dutch"]  == True:
            Base["language"].append("nl")

        ## Mentions
        others = data["other_advices"]
        current = data["number"]
        if current in others:
            others.remove(current)
        if isinstance(others,list):
            Base["mentions"] = others

        ## Types
        if data["doc_type_fr"] is not "not_found":
            Base["type"].append(data["doc_type_fr"])
        if data["doc_type_nl"] is not "not_found":
            Base["type"].append(data["doc_type_nl"])

        ## Persons
        Base["person"] = data["persons"]

        return Base

    def __format_date(self,date):
        return date[:10] + "T" + date[11:] + "Z"


