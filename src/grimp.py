#!/usr/bin/python3.4
## Pieterjan Montens 2016
## https://github.com/PieterjanMontens
import optparse
import json
import sys
from config_default import CFG
from grimp_importer import Importer
from solr_importer import SolrImporter




################################### MAIN STUFF
##############################################
def main():

    ############ Handle input & parameters
    p = optparse.OptionParser()
    p.add_option('--cfg','-c',action='store',default=None, help="Config to apply")
    p.add_option('--tool',action='store',default=None, help="Specify Tool [neo4j|Solr]")
    p.add_option('--test','-t',action='store_true',default=False, help="Test import")
    p.add_option('--quiet','-q',action="store_true", default=False, help="Quiet mode")
    p.add_option('--no_out',action="store_true", default=False, help="Disable pipe mode")
    options, arguments = p.parse_args()

    if options.cfg == None:
        print('\n Error: no config type specified.\n')
        exit(1)

    if options.tool == "neo4j":
        agent = Importer(CFG['NEO4j_URL'])
    elif options.tool == "solr":
        agent = SolrImporter()

    if options.test == True:
        agent.test_enable()

    ############ Read JSON Input
    for line in sys.stdin:
        try:
            json_input = json.loads(line)
            agent.imports(json_input)

            if not options.no_out:
                sys.stdout.write(line)

        except ValueError as e:
            if not options.quiet:
                print(e)
                print('\n Warning: line failed to parse, probably not valid json\n')
                print(line)

    ############ Job's done !
    exit(0)

if __name__ == '__main__':
  main()
