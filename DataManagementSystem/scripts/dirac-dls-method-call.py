'''
Created on Jun 9, 2015

@author: Corentin Berger
'''
from DIRAC.Core.Base import Script

lfn = None
name = None
fullFlag = False
after = None
before = None
status = None

Script.registerSwitch( '', 'Full', '   Print full method call' )
Script.registerSwitch( 'f:', 'File=', 'Name of LFN [%s]' % lfn )
Script.registerSwitch( 'm:', 'MethodName=', 'Name of method [%s]' % name )
Script.registerSwitch( 'a:', 'After=', 'date, format be like 1999-12-31 [%s]' % after )
Script.registerSwitch( 'b:', 'Before=', 'date, format be like 1999-12-31 [%s]' % before )
Script.registerSwitch( 'w:', 'Status=', 'date, format be like 1999-12-31 [%s]' % status )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] -l LFN -m NAME' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \nNAME : A method name' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "f" or switch[0].lower() == "file":
    lfn = switch[1]
  elif switch[0] == "m" or switch[0].lower() == "methodname":
    name = switch[1]
  elif switch[0] == "a" or switch[0].lower() == "after":
    after = switch[1]
  elif switch[0] == "b" or switch[0].lower() == "before":
    before = switch[1]
  elif switch[0] == "s" or switch[0].lower() == "status":
    status = switch[1]
  elif switch[0].lower() == "full":
    fullFlag = True


from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

def printMethodCallLFN( call, lfn, full = False, status = None ):
  callLines = []
  line = '%s %s, ' % \
    ( call.name.name, 'SequenceID %s ' % call.sequenceID )
  for action in call.actions :
    if action.fileDL.name == lfn:
      if status :
        if action.status == status :
          line += '%s%s%s%s'\
            % ( '%s ' % action.status,
                ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
                call.creationTime )
          if full :
            line += '%s%s'\
              % ( ',extra %s ' % action.extra if action.extra else '',
              ',errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
        callLines.append( line )
      else :
        line += '%s%s%s%s'\
            % ( '%s ' % action.status,
                ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ',targetSE %s ' % action.targetSE.name if action.targetSE else '',
                call.creationTime )
        if full :
          line += '%s%s'\
            % ( ',extra %s ' % action.extra if action.extra else '',
              ',errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
        callLines.append( line )
  return '\n'.join( callLines )


def printMethodCall( call, full = False, status = None ):
  callLines = []
  line = '%s %s %s' % \
    ( call.name.name, 'SequenceID %s ' % call.sequenceID, call.creationTime )
  callLines.append( line )
  for action in call.actions :
    if status :
        if action.status == status :
          line = '\t%s%s%s%s'\
              % ( '%s ' % action.status,
              ',file %s ' % action.fileDL.name if action.fileDL else '',
              ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
              ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
          if full :
            line += '%s%s'\
                % ( ',extra %s ' % action.extra if action.extra else '',
                  ',errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
          callLines.append( line )
    else :
      line = '\t%s%s%s%s'\
              % ( '%s ' % action.status,
              ',file %s ' % action.fileDL.name if action.fileDL else '',
              ',sourceSE %s ' % action.srcSE.name if action.srcSE else '',
              ',targetSE %s ' % action.targetSE.name if action.targetSE else '' )
      if full :
        line += '%s%s'\
            % ( ',extra %s ' % action.extra if action.extra else '',
              ',errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
      callLines.append( line )
  return '\n'.join( callLines )



args = Script.getPositionalArgs()

dlc = DataLoggingClient()

if not lfn and not name :
  print 'you should give at least one lfn or one method name'
else :
  if lfn :
    print status
    res = dlc.getMethodCallOnFile( lfn, before, after )
    if res['OK']:
      if not res['Value'] :
        print 'no methodCall to print'
      else :
        for call in res['Value'] :
          print printMethodCallLFN( call, lfn, fullFlag, status )
    else :
      print res['Value']
  elif name :
    res = dlc.getMethodCallByName( name, before, after )
    if res['OK']:
      if not res['Value'] :
        print 'no methodCall to print'
      else :
        for call in res['Value'] :
          print printMethodCall( call, fullFlag, status )
    else :
      print res['Value']

