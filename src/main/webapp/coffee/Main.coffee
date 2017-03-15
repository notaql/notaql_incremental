###
Main class

param options =
  pollingIntervallMillis    # The Intervall between polling-attemps from the server in milliseconds or false to disable polling
###
class Main
  @console = new Console({
    isShowingDebugMessages : true
  })


  constructor: (@options = {}) ->
    # Init the functionality
    @server = new Server()
    @init()

    Main.console.print("Dashboard geladen")


  ###
  Initializes everything
  ###
  init: ->
    # Get the engines from the server
    serverRequestGetEngines = new ServerRequest(ServerRequest.REQUEST_TYPE_GET_ENGINES)
    serverRequestGetEngines.addDoneFunction(@eventResponseGetEngines)

    # Load transformations which are stored at the server
    @loadStoredTransformationsFromServer()

    # Init the message poller
    if @options.pollingIntervallMillis? and @options.pollingIntervallMillis
      @messagePoller = new MessagePoller({
        pollingIntervallMillis : @options.pollingIntervallMillis
      })

    # Functionality for the buttons
    $("#nav a#buttonAddEngine").click( => @eventButtonPressedAddEngine() )
    $("#nav a#buttonConfiguration").click( => @eventButtonPressedConfiguration() )
    $("#nav a#buttonKillAll").click( => @eventButtonPressedKillAll() )
    $("#nav a#buttonStore").click( => @eventButtonPressedStore() )
    $("#nav a#buttonLoad").click( => @eventButtonPressedLoad() )


  ###
  Gets the engine with the given id

  param id
  return Engine or null
  ###
  getEngine: (id) ->
    unless @engines? or @engines[id]?
      return null
    else
      return @engines[id]


  ###
  Gets the engines on the dashboard

  return array (never null)
  ###
  getEngines: ->
    unless @engines?
      return []
    else
      enginesArray  = []
      for id, engine of @engines
        enginesArray.push(engine)
      return enginesArray


  ###
  Gets the transformation with the given id

  param id
  return Transformation or null
  ###
  getTransformation: (id) ->
    for transformation in @getTransformations()
      if transformation.id is id
        return transformation

    return null


  ###
  Gets the transformations on the dashboard

  return array (never null)
  ###
  getTransformations: ->
    transformations = []

    for engine in @getEngines()
      for transformation in engine.getTransformations()
        if $.inArray(transformation, transformations) is -1
          transformations.push(transformation)

    return transformations


  ###
  Get the options to be passed to new engines

  return options
  ###
  getEngineOptions: ->
    return {
      server : @server
      callbackClosed : [@callbackEngineClosed]
    }


  ###
  Registers an engine. If none is passed a new one will be created

  param engine The engine to register
  ###
  addEngine: (engine) ->
    unless engine?
      engine = new Engine(@getEngineOptions())

    unless @engines?
      @engines = {}
    @engines[engine.id] = engine

    return engine


  ###
  Triggered when the user clicks on the button which shows the config
  ###
  eventButtonPressedConfiguration: ->
    new ConfigurationWindow()


  ###
  Executed when the user clicks on the button which adds an engine
  ###
  eventButtonPressedAddEngine: ->
    @addEngine().moveToFront()


  ###
  Executed when the user clicks on the button which stores the dashboard.
  ###
  eventButtonPressedStore: ->
    serializedData = @serialize()
    serializedDataStringJson = JSON.stringify(serializedData)
    serializedDataStringJsonPretty = JSON.stringify(serializedData, null, 1)
    serializedDataStringBase64 = StoreWindow.encodeToBase64(serializedDataStringJson)
    StoreWindow.setLocalStorage(serializedDataStringBase64)

    new StoreWindow(serializedDataStringBase64, serializedDataStringJsonPretty)
    Main.console.print("Dashboard gespeichert")


  ###
  Executed when the user clicks on the button which loads the dashboard.
  ###
  eventButtonPressedLoad: ->
    new LoadWindow(@callbackLoadDashboard)


  ###
  Called by the LoadWindow in order to load serialized data
  ###
  callbackLoadDashboard: (callbackData) =>
    @deserialize(callbackData)
    Main.console.print("Gespeichertes Dashboard geladen")


  ###
  Loads the transformations which are stored server-side.
  ###
  loadStoredTransformationsFromServer: ->
    request = new ServerRequest(ServerRequest.REQUEST_TYPE_TRANSFORMATION_LOAD)
    request.addDoneFunction( (returnJson) => @eventResponseLoadStoredTransformationsFromServer(returnJson) )


  ###
  Triggered when the server responds with stored transformations.

  param returnJson the json returned by the server
  ###
  eventResponseLoadStoredTransformationsFromServer: (returnJson) ->
      if returnJson.response is "error"
        Main.console.error(returnJson.error_message)

      else if returnJson.response is "ok"
        # Wait for the engines
        unless @hasResponseWithEngines?
          setTimeout(=>
            @eventResponseLoadStoredTransformationsFromServer(returnJson)
          , 10)
          return

        # Deserialize
        for transformation_base64 in returnJson.list
          try
            decodedData = LoadWindow.decodeBase64(transformation_base64)
            @deserialize(decodedData, true)
          catch error
            Main.console.error("Base64-Eingabe ist fehlerhaft (#{error})")


  ###
  Serializes the dashboard into an array

  return array with the engines
  ###
  serialize: ->
    return (
      for engine in @getEngines()
        engine.serialize()
    )


  ###
  De-Serializes the dashboard from one object generated by Main.serialize().

  param serializedData all engines which shall be deserialized
  param areTriggersRunning wether the trigger-transformations shall be assumed to be already running (can only be assumed if loaded from the server-side storage)
  ###
  deserialize: (serializedData = [], areTriggersRunning = false) ->
    serializedTransformations = {}

    # De-serialze the Engines and also collect all transformations without duplicates
    for serializedEngine in serializedData
      engine = Engine.deserialize(serializedEngine, @getEngineOptions())
      @addEngine(engine)

      # Store the transformations without duplicates
      if serializedEngine.transformations?
        for serializedTransformation in serializedEngine.transformations
          unless serializedTransformations[serializedTransformation.id]?
            serializedTransformations[serializedTransformation.id] = serializedTransformation


    # Transformations
    generatedTransformations = []
    for id, serializedTransformation of serializedTransformations
      engineIn = @getEngine(serializedTransformation.engine_in)
      engineOut = @getEngine(serializedTransformation.engine_out)

      try
        transformation = new Transformation(engineIn, engineOut, {
          scriptBody : serializedTransformation.script_body
          triggerId : if serializedTransformation.engine_out? and areTriggersRunning then serializedTransformation.trigger_id else null
        })

        generatedTransformations.push(transformation)
      catch error
        Main.console.debug("Transformation konnte nicht geladen werden (#{error})")

    return generatedTransformations


  ###
  Triggered when the server responds with engines
  ###
  eventResponseGetEngines: (returnJson) =>
    if returnJson?.response is "ok"
      @server.setEngines(
        for returnJsonEngine in returnJson.list
          ServerEngine.createFromServerJson(returnJsonEngine)
      )
      @hasResponseWithEngines = true
      Main.console.debug("Engines: #{JSON.stringify(@server.getEngines())}")
    else
      Main.console.error("Fehler: Engines konnten nicht vom Server abgeholt werden")


  ###
  Called by the engine when its closed

  param engine
  ###
  callbackEngineClosed: (engine) =>
    delete @engines[engine.id]


  ###
  Triggered when the user clicks on the button which stops everything
  ###
  eventButtonPressedKillAll: ->
    if @getEngines().length is 0
      Main.console.error("Es gibt gar keine Transformationen die man stoppen koennte")

    else if confirm("Wirklich alle Transformationen abbrechen?")
      serverRequestKillAll = new ServerRequest(ServerRequest.REQUEST_TYPE_KILL_ALL)
      serverRequestKillAll.addDoneFunction(@eventResponseKillAll)


  ###
  Triggered when the servers responds to a kill_all request
  ###
  eventResponseKillAll: (returnJson) =>
    if returnJson?.response is "ok"
      Main.console.print("Alle Jobs angehalten")
      for engine in @getEngines()
        for transformation in engine.getTransformations()
          transformation.window?.options.content.clearExecution()
    else
      Main.console.error("Fehler: Jobs konnten nicht gekillt werden")


  ###
  Generates a random id to be used for decentral coordination

  The usual approach would be a uuid, However, for the current use-case
  less entropy is still more than enough. 6 alphanumeric values equal
  2176782336 possible combinations.

  return id
  ###
  @randomId: ->
    randomString = (length=1) -> (Math.random()+1).toString(36).substring(2,2+length)

    # Call twice for more randomness and add the last digit of the current time
    randomString(3) + randomString(3)


# Main entry point
main = new Main({
    pollingIntervallMillis : 10000
})
