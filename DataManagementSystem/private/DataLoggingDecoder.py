'''
Created on May 5, 2015

@author: Corentin Berger
'''

import json
from json import JSONDecoder

from DIRAC.DataManagementSystem.Client.DataLoggingAction import DataLoggingAction
from DIRAC.DataManagementSystem.Client.DataLoggingFile import DataLoggingFile
from DIRAC.DataManagementSystem.Client.DataLoggingSequence import DataLoggingSequence
from DIRAC.DataManagementSystem.Client.DataLoggingOperation import DataLoggingOperation
from DIRAC.DataManagementSystem.Client.DataLoggingStatus import DataLoggingStatus
from DIRAC.DataManagementSystem.Client.DataLoggingCaller import DataLoggingCaller


class DataLoggingDecoder( json.JSONDecoder ):

    def __init__( self, *args, **kargs ):
        JSONDecoder.__init__( self, object_hook = self.dict_to_object,
                             *args, **kargs )

    def dict_to_object( self, d ):
        # print 'decode %s' % d
        if '__type__' not in d:
            return d
        # print 'type %s'%__type__
        typeObj = d.pop( '__type__' )
        try:
            if typeObj == 'DataLoggingAction':
              obj = DataLoggingAction( d['file'], d['status'] )
              return obj

            if typeObj == 'DataLoggingSequence':
              obj = DataLoggingSequence.fromJSON( d['operations'][0], d['caller'] )
              return obj

            if typeObj == 'DataLoggingFile':
              obj = DataLoggingFile( d['name'] )
              return obj

            if typeObj == 'DataLoggingStatus':
              obj = DataLoggingStatus( d['name'] )
              return obj

            if typeObj == 'DataLoggingOperation':
              obj = DataLoggingOperation( d )
              obj.actions = d['Actions']
              obj.children = d['Children']
              return obj

            if typeObj == 'DataLoggingCaller':
              obj = DataLoggingCaller( d['name'] )
              return obj

            else:
              return d

        except:
            d['__type__'] = type
            return d


