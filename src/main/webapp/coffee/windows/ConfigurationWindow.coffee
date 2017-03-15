###
A window on the dashboard used for the notaql-configuration

param options
###
class ConfigurationWindow extends Window
  @DEFAULT_TITLE = "Konfiguration"


  constructor: (@options = {}) ->
    # Object variables
    @cachedKeyValues = {}

    # Set default parameters
    unless @options.title?
      @options.title = ConfigurationWindow.DEFAULT_TITLE

    @options.autoresize = true
    @options.classes = ["configurationwindow"]
    @options.content = new KeyValueContent(this, @options)
    @options.optimalSize = {
      width : true
      height : true
    }

    @requestKeyValues()

    super(@options)


  ###
  Gets the configuration from the server.
  Note: Not the current but the cached version will be send to the window. The
    window will change at the moment the server responds with the current values

  return the cached configuration
  ###
  requestKeyValues: ->
    serverRequestGetConfiguration = new ServerRequest(ServerRequest.REQUEST_TYPE_CONFIGURATION_GET)
    serverRequestGetConfiguration.addDoneFunction(@eventResponseGetConfiguration)

    return @cachedKeyValues


  ###
  Returns the currently cached values from the server.

  return the cached configuration
  ###
  getKeyValues: ->
    return @cachedKeyValues


  ###
  Triggered when the server responds with new values.
  Updates the content.

  param returnJson
  ###
  eventResponseGetConfiguration: (returnJson) =>
    if returnJson?.response is "ok"
      @cachedKeyValues = returnJson.map
      @options.content.createKeyValueInputs()
    else
      Main.console.error("Fehler: Konfiguration konnten nicht vom Server abgeholt werden")


  ###
  Sends the configuration to the server

  param keyValues
  ###
  setKeyValues: (keyValues) ->
    @options.content.nodeButtonStore.text("Speichere...")

    serverRequestSetConfiguration = new ServerRequest(ServerRequest.REQUEST_TYPE_CONFIGURATION_SET, keyValues)
    serverRequestSetConfiguration.addDoneFunction(@eventResponseSetConfiguration)


  ###
  Triggered when the server responds to the set_configuration request.

  param returnJson
  ###
  eventResponseSetConfiguration: (returnJson) =>
    if returnJson?.response is "ok"
      @close()
      Main.console.print("Neue Konfiguration gespeichert und geladen")
    else
      @options.content.nodeButtonStore.text("Speichern")
      Main.console.error("Fehler: Konfiguration konnten nicht beim Server gespeichert werden")
