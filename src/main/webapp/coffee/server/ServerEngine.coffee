###
Wraps informations about a specific engine at the server

param typeName    # The name of the engine (e.g. "csv")
param arguments   # The arguments for the engine (e.g. "csv_path")
###
class ServerEngine
  constructor: (@typeName, @arguments, @requiredArguments=[], @advisorArguments=[]) ->


  ###
  Creates a ServerEngine-instance based on the json which is delivered by the server

  param json server-response
  ###
  @createFromServerJson: (json) ->
    return new ServerEngine(json.typeName, json.arguments, json.requiredArguments, json.advisorArguments)


  ###
  Checks if the given argument is required

  param argument a string
  ###
  isArgumentRequired: (argument) ->
    if argument in @requiredArguments
      return true
    else
      return false
