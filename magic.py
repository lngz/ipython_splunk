from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

import sys
from pprint import pprint
import socket

import splunklib.client as client
import splunklib.results as results

from splunklib.binding import HTTPError

from tabulate import tabulate


FLAGS_CREATE = [
    "earliest_time", "latest_time", "now", "time_format",
    "exec_mode", "search_mode", "rt_blocking", "rt_queue_size",
    "rt_maxblocksecs", "rt_indexfilter", "id", "status_buckets",
    "max_count", "max_time", "timeout", "auto_finalize_ec", "enable_lookups",
    "reload_macros", "reduce_freq", "spawn_process", "required_field_list",
    "rf", "auto_cancel", "auto_pause",
]

verbose = 0

def dslice(value, *args):
    """Returns a 'slice' of the given dictionary value containing only the
       requested keys. The keys can be requested in a variety of ways, as an
       arg list of keys, as a list of keys, or as a dict whose key(s) represent
       the source keys and whose corresponding values represent the resulting 
       key(s) (enabling key rename), or any combination of the above.""" 
    result = {}
    for arg in args:
        if isinstance(arg, dict):
            for k, v in six.iteritems(arg):
                if k in value: 
                    result[v] = value[k]
        elif isinstance(arg, list):
            for k in arg:
                if k in value: 
                    result[k] = value[k]
        else:
            if arg in value: 
                result[arg] = value[arg]
    return result
def pretty(response):
    reader = results.ResultsReader(response)
    print(tabulate(reader, headers="keys", tablefmt="psql"))






# The class MUST call this class decorator at creation time
@magics_class
class SplunkMagics(Magics):

    @line_magic('connect')
    def connect(self,line):
        [HOST,PORT,USERNAME,PASSWORD] = line.split(" ")
        self.service = client.connect(
                host=HOST,
                port=PORT,
                username=USERNAME,
                password=PASSWORD)

    @line_magic('spl')
    @cell_magic('spl')
    def spl(self, line):
        "my line magic"
        # print("Full access to the main IPython object:", self.shell)
        # print("Variables in the user namespace:", list(self.shell.user_ns.keys()))
        outputs = []
        kwargs_create = {}
        kwargs_results = {'output_mode': 'csv', 'count': 0}
        try:
            self.service.parse(line, parse_only=True)
        except HTTPError as e:
            print("query '%s' is invalid:\n\t%s" % (line, str(e)), 2)
            return

        job = self.service.jobs.create(line, **kwargs_create)
        while True:
            while not job.is_ready():
                pass
            stats = {'isDone': job['isDone'],
                     'doneProgress': job['doneProgress'],
                     'scanCount': job['scanCount'],
                     'eventCount': job['eventCount'],
                     'resultCount': job['resultCount']}
            progress = float(stats['doneProgress'])*100
            scanned = int(stats['scanCount'])
            matched = int(stats['eventCount'])
            results = int(stats['resultCount'])
            if verbose > 0:
                status = ("\r%03.1f%% | %d scanned | %d matched | %d results" % (
                    progress, scanned, matched, results))
                outputs.append(status)
                
            if stats['isDone'] == '1': 
                if verbose > 0: outputs.append('\n')
                break
            # sleep(2)

        if 'count' not in kwargs_results: kwargs_results['count'] = 0
        results = job.results(**kwargs_results)
        while True:
            content = results.read(1024)
            if len(content) == 0: break
            outputs.append(content.decode('utf-8'))
            
        outputs.append('\n')

        job.cancel()
        return outputs

    @line_magic('oneshot')
    @cell_magic('oneshot')
    def oneshot(self, line):
        "my line magic"
        # print("Full access to the main IPython object:", self.shell)
        # print("Variables in the user namespace:", list(self.shell.user_ns.keys()))
        outputs = []

        socket.setdefaulttimeout(None)
        response = self.service.jobs.oneshot(line)

        pretty(response)
        return outputs

    @line_magic('getapp')
    def getapp(self,line):
        apps = []
        for app in self.service.apps:
            apps.append(app.name)
        return apps

    @line_magic('createsavedsearch')
    def createsavedsearch(self,line):
        savedsearches = self.service.saved_searches

        # Create a saved search
        name = "test"
        mysearch = savedsearches.create(name, line)
        return line
 
    @line_magic('listsavedsearches')
    def listsavedsearches(self,line):
        result = []
        for savedsearch in self.service.saved_searches:
            result.append( savedsearch.name )

            result.append (savedsearch.content["search"])
            
        return result

def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(SplunkMagics)
