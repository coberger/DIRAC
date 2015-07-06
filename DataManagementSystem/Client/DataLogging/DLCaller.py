'''
Created on May 8, 2015

@author: Corentin Berger
'''
from DIRAC.DataManagementSystem.private.DLJSON import DLJSON


class DLCaller( DLJSON ):

  attrNames = ['ID', 'name']

  def __init__( self, name ):
    self.name = name
