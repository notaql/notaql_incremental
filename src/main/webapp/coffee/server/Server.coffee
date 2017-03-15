###
Wraps informations about the server (e.g. available engines)

param options
###
class Server
  constructor: () ->


  ###
  Gets the available engines

  return [ServerEngine]
  ###
  getEngines: ->
    if @engines?
      return @engines
    else
      return []


  ###
  Gets the specified engine by its type (=name)

  return ServerEngine or null
  ###
  getEngine: (typeName) ->
    for engine in @getEngines()
      if engine.typeName is typeName
        return engine

    return null


  ###
  Stores available engines

  param serverEngines [ServerEngine]
  ###
  setEngines: (serverEngines) ->
    @engines = serverEngines
