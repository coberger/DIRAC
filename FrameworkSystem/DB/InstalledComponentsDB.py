"""
Classes and functions for easier management of the InstalledComponents database
"""

__RCSID__ = "$Id$"

import datetime
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from sqlalchemy import MetaData, \
                        Column, \
                        Integer, \
                        String, \
                        DateTime, \
                        create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, \
                           scoped_session, \
                           relationship
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql.expression import null

metadata = MetaData()
Base = declarative_base()

class Component( Base ):
  """
  This class defines the schema of the Components table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Components'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  componentID = Column( 'ComponentID', Integer, primary_key = True )
  system = Column( 'System', String( 32 ), nullable = False )
  module = Column( 'Module', String( 32 ), nullable = False )
  cType = Column( 'Type', String( 32 ), nullable = False )

  def __init__( self, system = null(), module = null(), cType = null() ):
    self.system = system
    self.module = module
    self.cType = cType

  def fromDict( self, dictionary ):
    """
    Fill the fields of the Component object from a dictionary
    The dictionary may contain the keys: ComponentID, System, Module, Type
    """

    self.componentID = dictionary.get( 'ComponentID', self.componentID )
    self.system = dictionary.get( 'System', self.system )
    self.module = dictionary.get( 'Module', self.module )
    self.cType = dictionary.get( 'Type', self.cType )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeInstallations = False, includeHosts = False ):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in which this Component is included
    If includeHosts is also True, further information about the Hosts where the
    installations are is included
    """

    dictionary = {
                  'ComponentID': self.componentID,
                  'System': self.system,
                  'Module': self.module,
                  'Type': self.cType
                  }

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = installation.toDict( False, includeHosts )[ 'Value' ]
        installations.append( relationshipDict )
      dictionary[ 'Installations' ] = installations

    return S_OK( dictionary )

class Host( Base ):
  """
  This class defines the schema of the Hosts table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Hosts'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  hostID = Column( 'HostID', Integer, primary_key = True )
  hostName = Column( 'HostName', String( 32 ), nullable = False )
  cpu = Column( 'CPU', String( 64 ), nullable = False )
  installationList = relationship( 'InstalledComponent',
                                    backref = 'installationHost' )

  def __init__( self, host = null(), cpu = null() ):
    self.hostName = host
    self.cpu = cpu

  def fromDict( self, dictionary ):
    """
    Fill the fields of the Host object from a dictionary
    The dictionary may contain the keys: HostID, HostName, CPU
    """

    self.hostID = dictionary.get( 'HostID', self.hostID )
    self.hostName = dictionary.get( 'HostName', self.hostName )
    self.cpu = dictionary.get( 'CPU', self.cpu )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeInstallations = False, includeComponents = False ):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in this Host
    If includeComponents is also True, further information about which
    Components where installed is included
    """

    dictionary = {
                  'HostID': self.hostID,
                  'HostName': self.hostName,
                  'CPU': self.cpu
                  }

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = \
                      installation.toDict( includeComponents, False )[ 'Value' ]
        installations.append( relationshipDict )
      dictionary[ 'Installations' ] = installations

    return S_OK( dictionary )

class InstalledComponent( Base ):
  """
  This class defines the schema of the InstalledComponents table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'InstalledComponents'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  componentID = Column( 'ComponentID',
                        Integer,
                        ForeignKey( 'Components.ComponentID' ),
                        primary_key = True )
  hostID = Column( 'HostID',
                    Integer,
                    ForeignKey( 'Hosts.HostID' ),
                    primary_key = True )
  instance = Column( 'Instance',
                      String( 32 ),
                      primary_key = True )
  installationTime = Column( 'InstallationTime',
                              DateTime,
                              primary_key = True )
  unInstallationTime = Column( 'UnInstallationTime',
                                DateTime )
  installationComponent = relationship( 'Component',
                                        backref = 'installationList' )

  def __init__( self, instance = null(),
                      installationTime = null(),
                      unInstallationTime = null() ):
    self.instance = instance
    self.installationTime = installationTime
    self.unInstallationTime = unInstallationTime

  def fromDict( self, dictionary ):
    """
    Fill the fields of the InstalledComponent object from a dictionary
    The dictionary may contain the keys: ComponentID, HostID, Instance,
    InstallationTime, UnInstallationTime
    """

    self.componentID = dictionary.get( 'ComponentID', self.componentID )
    self.hostID = dictionary.get( 'HostID', self.hostID )
    self.instance = dictionary.get( 'Instance', self.instance )
    self.installationTime = dictionary.get( 'InstallationTime',
                                            self.installationTime )
    self.unInstallationTime = dictionary.get( 'UnInstallationTime',
                                              self.unInstallationTime )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeComponents = False, includeHosts = False ):
    """
    Return the object as a dictionary
    If includeComponents is True, information about which Components where
    installed is included
    If includeHosts is True, information about the Hosts where the
    installations are is included
    """

    dictionary = {
                  'Instance': self.instance,
                  'InstallationTime': self.installationTime,
                  'UnInstallationTime': self.unInstallationTime
                  }

    if includeComponents:
      dictionary[ 'Component' ] = self.installationComponent.toDict()[ 'Value' ]
    else:
      dictionary[ 'ComponentID' ] = self.componentID

    if includeHosts:
      dictionary[ 'Host' ] = self.installationHost.toDict()[ 'Value' ]
    else:
      dictionary[ 'HostID' ] = self.hostID

    return S_OK( dictionary )

class InstalledComponentsDB( object ):
  """
  Class used to work with the InstalledComponentsDB database.
  It creates the tables on initialization and allows inserting, querying,
  deleting from/to the tables
  """

  def __init__( self ):
    self.__initializeConnection( 'Framework/InstalledComponentsDB' )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

  def __initializeConnection( self, dbPath ):

    result = getDBParameters( dbPath )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.host = dbParameters[ 'Host' ]
    self.port = dbParameters[ 'Port' ]
    self.user = dbParameters[ 'User' ]
    self.password = dbParameters[ 'Password' ]
    self.db = dbParameters[ 'DBName' ]

    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' %
                    ( self.user, self.password, self.host, self.port, self.db ),
                    pool_recycle = 3600, echo_pool = True
                    )
    self.Session = scoped_session( sessionmaker( bind = self.engine ) )
    self.inspector = Inspector.from_engine( self.engine )

  def __initializeDB( self ):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    # Components
    if not 'Components' in tablesInDB:
      try:
        Component.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'Components\' already exists' )

    # Hosts
    if not 'Hosts' in tablesInDB:
      try:
        Host.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'Hosts\' already exists' )

    # InstalledComponents
    if not 'InstalledComponents' in tablesInDB:
      try:
        InstalledComponent.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'InstalledComponents\' already exists' )

    return S_OK( 'Tables created' )

  def __filterFields( self, session, table, matchFields = {} ):
    """
    Filters instances of a selection by finding matches on the given fields
    session argument is a Session instance used to retrieve the items
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    matchFields accepts fields of the form <Field.bigger> and <Field.smaller>
    to filter using > and < relationships.
    If matchFields is empty, no filtering will be done
    """

    filtered = session.query( table )

    for key in matchFields.keys():
      actualKey = key

      comparison = '='
      if '.bigger' in key:
        comparison = '>'
        actualKey = key.replace( '.bigger', '' )
      elif '.smaller' in key:
        comparison = '<'
        actualKey = key.replace( '.smaller', '' )

      if matchFields[ key ] == None:
        sql = '%s IS NULL' % ( actualKey )
      elif type( matchFields[ key ] ) == list:
        if len( matchFields[ key ] ) > 0 and not None in matchFields[ key ]:
          sql = '%s IN ( ' % ( actualKey )
          for i, element in enumerate( matchFields[ key ] ):
            toAppend = element
            if type( toAppend ) == datetime.datetime:
              toAppend = toAppend.strftime( "%Y-%m-%d %H:%M:%S" )
            if type( toAppend in [ datetime.datetime, str ] ):
              toAppend = '\'%s\'' % ( toAppend )
            if i == 0:
              sql = '%s%s' % ( sql, toAppend )
            else:
              sql = '%s, %s' % ( sql, toAppend )
          sql = '%s )' % ( sql )
        else:
          continue
      elif type( matchFields[ key ] ) == str:
        sql = '%s %s \'%s\'' % ( actualKey, comparison, matchFields[ key ] )
      elif type( matchFields[ key ] ) == datetime.datetime:
        sql = '%s %s \'%s\'' % \
                        ( actualKey,
                          comparison,
                          matchFields[ key ].strftime( "%Y-%m-%d %H:%M:%S" ) )
      else:
        sql = '%s %s %s' % ( actualKey, comparison, matchFields[ key ] )

      filteredTemp = filtered.filter( sql )
      try:
        session.execute( filteredTemp )
        session.commit()
      except Exception, e:
        return S_ERROR( 'Could not filter the fields: %s' % ( e ) )
      filtered = filteredTemp

    return S_OK( filtered )

  def __filterInstalledComponentsFields( self, session, matchFields = {} ):
    """
    Filters instances by finding matches on the given fields in the same way
    as the '__filterFields' function
    The main difference with '__filterFields' is that this function is
    targeted towards the InstalledComponents table
    and accepts fields of the form <Component.Field> and <Host.Field>
    ( e.g., 'Component.System' ) to filter installations using attributes
    from their associated Components and Hosts.
    session argument is a Session instance used to retrieve the items
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships.
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    """

    componentKeys = {}
    for ( key, val ) in matchFields.items():
      if 'Component.' in key:
        componentKeys[ key.replace( 'Component.', '' ) ] = val

    hostKeys = {}
    for ( key, val ) in matchFields.items():
      if 'Host.' in key:
        hostKeys[ key.replace( 'Host.', '' ) ] = val

    selfKeys = {}
    for ( key, val ) in matchFields.items():
      if not 'Component.' in key and not 'Host.' in key:
        selfKeys[ key ] = val

    # Get the matching components
    result = self.__filterFields( session, Component, componentKeys )
    if not result[ 'OK' ]:
      return result

    componentIDs = []
    for component in result[ 'Value' ]:
      componentIDs.append( component.componentID )

    # Get the matching hosts
    result = self.__filterFields( session, Host, hostKeys )
    if not result[ 'OK' ]:
      return result

    hostIDs = []
    for host in result[ 'Value' ]:
      hostIDs.append( host.hostID )

    # Get the matching InstalledComponents
    result = self.__filterFields( session, InstalledComponent, selfKeys )
    if not result[ 'OK' ]:
      return result

    # And use the Component and Host IDs to filter them as well
    installations = result[ 'Value' ]\
                  .filter( InstalledComponent.componentID.in_( componentIDs ) )\
                  .filter( InstalledComponent.hostID.in_( hostIDs ) )

    return S_OK( installations )

  def exists( self, table, matchFields ):
    """
    Checks whether an instance matching the given criteria exists
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    matchFields may contain entries of the form 'Component.attribute' or
    'Host.attribute' if table equals InstalledComponent
    """

    session = self.Session()

    if table == InstalledComponent:
      result = self.__filterInstalledComponentsFields( session, matchFields )
    else:
      result = self.__filterFields( session, table, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result
    query = result[ 'Value' ]

    session.commit()
    session.close()

    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def addComponent( self, newComponent ):
    """
    Add a new component to the database
    newComponent argument should be a dictionary with the Component fields and
    its values

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    component = Component()
    component.fromDict( newComponent )

    try:
      session.add( component )
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not add Component: %s' % ( e ) )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Component successfully added' )

  def removeComponents( self, matchFields = {} ):
    """
    Removes components with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    matchFields argument can be empty to remove all the components

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    result = self.__filterFields( session, Component, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    for component in result[ 'Value' ]:
      session.delete( component )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Components successfully removed' )

  def getComponents( self,
                        matchFields = {},
                        includeInstallations = False,
                        includeHosts = False ):
    """
    Returns a list with all the components with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in
    which the components takes part is to be retrieved
    includeHosts (only if includeInstallations is set to True) indicates
    whether data about the host in which there are instances of this component
    is to be retrieved
    """

    session = self.Session()

    result = self.__filterFields( session, Component, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    components = result[ 'Value' ]
    if not components:
      session.rollback()
      session.close()
      return S_ERROR( 'No matching Components were found' )

    dictComponents = []
    for component in components:
      dictComponents.append \
          ( component.toDict( includeInstallations, includeHosts )[ 'Value' ] )

    session.commit()
    session.close()
    return S_OK( dictComponents )

  def getComponentByID( self, cId ):
    """
    Returns a component given its id
    """

    result = self.getComponents( matchFields = { 'ComponentID': cId } )
    if not result[ 'OK' ]:
      return result

    component = result[ 'Value' ]
    if component.count() == 0:
      return S_ERROR( 'Component with ID %s does not exist' % ( cId ) )

    return S_OK( component[0] )

  def componentExists( self, component ):
    """
    Checks whether the given component exists in the database or not
    """

    session = self.Session()

    try:
      query = session.query( Component )\
                          .filter( Component.system == component.system )\
                          .filter( Component.module == component.module )\
                          .filter( Component.cType == component.cType )
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 
                  'Couldn\'t check the existence of the component: %s' % ( e ) )

    session.commit()
    session.close()
    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def updateComponents( self, matchFields = {}, updates = {} ):
    """
    Updates Components objects on the database
    matchFields argument should be a dictionary with the fields to match
    (instances matching the fields will be updated) or empty to update all
    the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships updates argument
    should be a dictionary with the Component fields and their new
    updated values
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.Session()

    result = self.__filterFields( session, Component, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    components = result[ 'Value' ]

    for component in components:
      component.fromDict( updates )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Component(s) updated' )

  def addHost( self, newHost ):
    """
    Add a new host to the database
    host argument should be a dictionary with the Host fields and its values

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    host = Host()
    host.fromDict( newHost )

    try:
      session.add( host )
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not add Host: %s' % ( e ) )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Host successfully added' )

  def removeHosts( self, matchFields = {} ):
    """
    Removes hosts with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    matchFields argument can be empty to remove all the hosts

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    result = self.__filterFields( session, Host, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    for host in result[ 'Value' ]:
      session.delete( host )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Hosts successfully removed' )

  def getHosts( self,
                  matchFields = {},
                  includeInstallations = False,
                  includeComponents = False ):
    """
    Returns a list with all the hosts with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in
    which the host takes part is to be retrieved
    includeComponents (only if includeInstallations is set to True) indicates
    whether data about the components installed into this host is to
    be retrieved
    """

    session = self.Session()

    result = self.__filterFields( session, Host, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    hosts = result[ 'Value' ]
    if not hosts:
      session.rollback()
      session.close()
      return S_ERROR( 'No matching Hosts were found' )

    dictHosts = []
    for host in hosts:
      dictHosts.append \
          ( host.toDict( includeInstallations, includeComponents )[ 'Value' ] )

    session.commit()
    session.close()

    return S_OK( dictHosts )

  def getHostByID( self, cId ):
    """
    Returns a host given its id
    """

    result = self.getHosts( matchFields = { 'HostID': cId } )
    if not result[ 'OK' ]:
      return result

    host = result[ 'Value' ]
    if host.count() == 0:
      return S_ERROR( 'Host with ID %s does not exist' % ( cId ) )

    return S_OK( host[0] )

  def hostExists( self, host ):
    """
    Checks whether the given host exists in the database or not
    """

    session = self.Session()

    try:
      query = session.query( Host )\
                          .filter( Host.hostName == host.hostName )\
                          .filter( Host.cpu == host.cpu )
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not check the existence of the host: %s' % ( e ) )

    session.commit()
    session.close()
    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def updateHosts( self, matchFields = {}, updates = {} ):
    """
    Updates Hosts objects on the database
    matchFields argument should be a dictionary with the fields to
    match (instances matching the fields will be updated) or empty to update
    all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships updates argument
    should be a dictionary with the Host fields and their new updated values
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.Session()

    result = self.__filterFields( session, Host, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    hosts = result[ 'Value' ]

    for host in hosts:
      host.fromDict( updates )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'Host(s) updated' )

  def addInstalledComponent( self,
                                newInstallation,
                                componentDict,
                                hostDict,
                                forceCreate = False ):
    """
    Add a new installation of a component to the database
    installation argument should be a dictionary with the InstalledComponent
    fields and its values
    componentDict argument should be a dictionary with the Component fields
    and its values
    hostDict argument should be a dictionary with the Host fields and
    its values
    If forceCreate is set to True, both the component and the host will be
    created if they do not exist

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    installation = InstalledComponent()
    installation.fromDict( newInstallation )

    result = self.__filterFields( session, Component, componentDict )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result
    if result[ 'Value' ].count() != 1:
      if result[ 'Value' ].count() > 1:
        session.rollback()
        session.close()
        return S_ERROR( 'Too many Components match the criteria' )
      if result[ 'Value' ].count() < 1:
        if not forceCreate:
          session.rollback()
          session.close()
          return S_ERROR( 'Given component does not exist' )
        else:
          component = Component()
          component.fromDict( componentDict )
    else:
      component = result[ 'Value' ][0]


    result = self.__filterFields( session, Host, hostDict )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result
    if result[ 'Value' ].count() != 1:
      if result[ 'Value' ].count() > 1:
        session.rollback()
        session.close()
        return S_ERROR( 'Too many Hosts match the criteria' )
      if result[ 'Value' ].count() < 1:
        if not forceCreate:
          session.rollback()
          session.close()
          return S_ERROR( 'Given host does not exist' )
        else:
          host = Host()
          host.fromDict( hostDict )
    else:
      host = result[ 'Value' ][0]

    if component:
      installation.installationComponent = component
    if host:
      installation.installationHost = host

    try:
      session.add( installation )
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not add installation: %s' % ( e ) )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'InstalledComponent successfully added' )

  def getInstalledComponents( self,
                                  matchFields = {},
                                  installationsInfo = False ):
    """
    Returns a list with all the InstalledComponents with matches in the given
    fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances and may contain entries of the form
    'Component.attribute' or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    installationsInfo indicates whether information about the components and
    host taking part in the installation is to be provided
    """

    session = self.Session()

    result = self.__filterInstalledComponentsFields( session, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    installations = result[ 'Value' ]
    if not installations:
      session.rollback()
      session.close()
      return S_ERROR( 'No matching Installations were found' )

    dictInstallations = []
    for installation in installations:
      dictInstallations.append \
      ( installation.toDict( installationsInfo, installationsInfo )[ 'Value' ] )

    session.commit()
    session.close()

    return S_OK( dictInstallations )

  def updateInstalledComponents( self, matchFields = {}, updates = {} ):
    """
    Updates installations matching the given criteria
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances and may contain entries of the form
    'Component.attribute' or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.Session()

    result = self.__filterInstalledComponentsFields( session, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    installations = result[ 'Value' ]

    for installation in installations:
      installation.fromDict( updates )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'InstalledComponent(s) updated' )

  def removeInstalledComponents( self, matchFields = {} ):
    """
    Removes InstalledComponents with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match and may contain entries of the form 'Component.attribute'
    or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships.
    matchFields argument can be empty to remove all the hosts

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.Session()

    result = self.__filterInstalledComponentsFields( session, matchFields )
    if not result[ 'OK' ]:
      session.rollback()
      session.close()
      return result

    installations = result[ 'Value' ]

    for installation in installations:
      session.delete( installation )

    try:
      session.commit()
    except Exception, e:
      session.rollback()
      session.close()
      return S_ERROR( 'Could not commit changes: %s' % ( e ) )

    session.close()
    return S_OK( 'InstalledComponents successfully removed' )
