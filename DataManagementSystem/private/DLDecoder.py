'''
Created on May 5, 2015

@author: Corentin Berger
'''

import json
from json import JSONDecoder

from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement


class DLDecoder( json.JSONDecoder ):

    def __init__( self, *args, **kargs ):
        JSONDecoder.__init__( self, object_hook = self.dict_to_object,
                             *args, **kargs )

    def dict_to_object( self, d ):
        # print 'decode %s' % d
        if '__type__' not in d:
            return d
        # print 'type %s' % d['__type__']
        typeObj = d.pop( '__type__' )
        try:
            if typeObj == 'DLAction':
              obj = DLAction( d['file'], d['status'] , d['srcSE'], d['targetSE'], d['blob'], d['messageError'], ID = d['actionID'] )
              return obj
            elif typeObj == 'DLSequence':
              obj = DLSequence.fromJSON( d['MethodCalls'][0], d['caller'], d['sequenceID'] )
              return obj
            elif typeObj == 'DLFile':
              obj = DLFile( d['name'] )
              return obj
            elif typeObj == 'DLStatus':
              obj = DLStatus( d['name'] )
              return obj
            elif typeObj == 'DLMethodCall':
              obj = DLMethodCall( d )
              obj.actions = d['Actions']
              obj.children = d['Children']
              return obj
            elif typeObj == 'DLCaller':
              obj = DLCaller( d['name'] )
              return obj
            elif typeObj == 'DLMethodName':
              obj = DLMethodName( d['name'] )
              return obj
            elif typeObj == 'DLStorageElement':
              obj = DLStorageElement( d['name'] )
              return obj
            else:
              return d

        except Exception as e:
          print 'exception in decoder %s' % e
          d['__type__'] = type
          return d


