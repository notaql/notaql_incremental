###
A transformation which is currently displayed on the dashboard (with a Line)

param engineIn        # IN-ENGINE
param engineOut       # OUT-ENGINE
param options
  scriptBody          # The initial script body
  triggerId           # The trigger-id (used to identify the server-side trigger-transformation)
###
class Transformation extends Line
  constructor: (@engineIn, @engineOut, @options = {}) ->
    # Object variables
    @id = Main.randomId()

    # Check if the transformation is valid
    unless @isValidConnection()
      throw "Invalid transformation"

    # Create the line
    super(@options)
    @redraw()

    # Init with passed parameters
    if @options.scriptBody?
      @setScriptBody(@options.scriptBody)

    # Register at the engines
    @engineIn.registerTransformation(this, true)
    @engineOut.registerTransformation(this, false)

    # Register the events
    @nodeLine.click(=> @eventFocusGained())

    # Trigger-based transformation
    if @options.triggerId?
      @setTriggerTransformationId(@options.triggerId)


  ###
  Serializes this transformation-object

  return one object containing everything needed for restoring the object
  ###
  serialize: ->
    return {
      id : @id
      engine_in : @engineIn.id
      engine_out : @engineOut.id
      trigger_id : @triggerTransformationId if @triggerTransformationId?
      script_header : @generateScriptHeader()
      script_body : @getScriptBody()
    }


  ###
  Sets the script body (the user input)

  param newScriptBody
  ###
  setScriptBody: (newScriptBody = "") ->
    unless @window?
      @window = new TransformationWindow(this, {
          hidden : true
      })

    @window?.options.content.setScriptBody(newScriptBody)


  ###
  Sets the trigger-transformation id

  param new id
  ###
  setTriggerTransformationId: (id) ->
    unless @window?
      @window = new TransformationWindow(this, {
          hidden : true
      })

    @triggerTransformationId = id
    @window?.options.content.nodeButtonStart.text("Stoppen")


  ###
  Generates the lines with the engine-parameters

  return string with the engine lines or empty string
  ###
  generateScriptHeader: ->
    lineIn = ""
    if @engineIn.generateEngineLine()
      lineIn = "IN-ENGINE: #{@engineIn.generateEngineLine()},"

    lineOut = ""
    if @engineOut.generateEngineLine()
      lineOut = "OUT-ENGINE: #{@engineOut.generateEngineLine()},"

    if lineOut is ""
      return lineIn
    else
      return "#{lineIn}\n#{lineOut}"


  ###
  Gets the script body (the user input)

  return string with the script body or empty string
  ###
  getScriptBody: ->
    scriptBody = ""
    if @window?
      scriptBody = @window?.options.content.getScriptBody()

    return scriptBody


  ###
  Generates the complete notaql-script which shall be send to the server
  ###
  getScript: ->
    return @generateScriptHeader() + @getScriptBody()


  ###
  Redraws the transformation line. Basically a proxy to Line.redraw() to generate
  the parameters for this function.
  ###
  redraw: ->
    posStart = @engineIn.nodeRightConnectionPoint.offset()
    posEnd = @engineOut.nodeLeftConnectionPoint.offset()
    super(posStart.left, posStart.top, posEnd.left, posEnd.top)


  ###
  Deletes the transformation and takes care of the associated engines and there stored reference

  Also removes the line from the dashboard
  ###
  close: ->
    # Set the engines back to normal mode
    if @window?.options.content.automate? and @window?.options.content.automate
      @engineIn.options.content.eventSetAutomated(false)
      @engineOut.options.content.eventSetAutomated(false)

    # Remove the line and the window from the dashboard
    super()
    @window?.close()

    # Remove the references
    @engineIn.unregisterTransformation(this)
    @engineOut.unregisterTransformation(this)
    @stopTriggerTransformation()


  ###
  Stops the currently running trigger-based transformation
  ###
  stopTriggerTransformation:  ->
    if @triggerTransformationId?
      @window?.options.content.nodeButtonStart.text("Stoppe...")
      request = new ServerRequest(ServerRequest.REQUEST_TYPE_KILL, { id : @triggerTransformationId })
      request.addDoneFunction( (returnJson) => @eventTriggerTransformationStopped(returnJson) )


  ###
  Triggered when a new trigger-based transformation was started

  param returnJson the json-data returned by the server
  ###
  eventTriggerTransformationStarted: (returnJson) ->
    @triggerTransformationId = returnJson.trigger_id
    @storeServerSide("trigger_#{@triggerTransformationId}")
    @window?.options.content.nodeButtonStart.text("Stoppen")
    Main.console.print("Trigger-Transformation gestartet (ID: #{@triggerTransformationId})")


  ###
  Triggered when the trigger-based transformation was stopped

  param returnJson the json-data returned by the server
  ###
  eventTriggerTransformationStopped: (returnJson) ->
    if returnJson.response is "error"
      Main.console.error(returnJson.error_message)

    else if returnJson.response is "ok"
      Main.console.print("Trigger-Transformation gestoppt (ID: #{@triggerTransformationId})")
      @removeServerSide("trigger_#{@triggerTransformationId}")
      @window?.options.content.clearExecution()


  ###
  Event is triggered when the transformation gains focus. For example because it is clicked.
  ###
  eventFocusGained: ->
    @showWindow()


  ###
  Event is triggered when the transformation looses focus. For example because another transformation is clicked.
  ###
  eventFocusLost: ->


  ###
  Checks if the transformation is valid at creation-time (e.g. different in and out engines)

  return true if it is valid, false otherwise
  ###
  isValidConnection: ->
    # Check if the engines are identical
    if @engineIn is @engineOut
      Main.console.error("Engines muessen unterschiedlich sein")
      return false

    # Check if one of the other engines already has another connection established
    if @engineIn.isInEngine() || @engineOut.isOutEngine()
      Main.console.error("Beide Engines muessen einen freien Connector haben")
      return false

    # Check for cycles in the graph
    if @engineIn.isConnectedTo(@engineOut)
      Main.console.error("Es darf kein Zyklus entstehen")
      return false

    return true


  ###
  Checks if the transformation is valid at execution-time (e.g. in-engine and out-engine as specified)

  return true if it is valid, false otherwise
  ###
  isValidExecution: ->
    # Check if the engines are identical
    if not @engineIn.type? or not @engineOut.type?
      Main.console.error("Engine-Typ muss auf beiden Seiten spezifiziert sein")
      return false

    # Check if all required arguments in the engines as specified
    if not @engineIn.areRequiredArgumentsSet() or not @engineOut.areRequiredArgumentsSet()
      Main.console.error("Bei beiden Engines muessen alle Pflichtfelder (Felder mit *) ausgefuellt sein")
      return false

    else
      return true


  ###
  Shows the window which belongs to this transformation
  ###
  showWindow: ->
    if @window?
      @window.setIsDisplayed(true)
    else
      @window = new TransformationWindow(this)


  ###
  Stores the transformation

  param key the key used for the key-value store for the transformations
  ###
  storeServerSide: (key) ->
    serializedData = [@engineIn.serialize(), @engineOut.serialize()]

    serializedDataStringJson = JSON.stringify(serializedData)
    serializedDataStringBase64 = StoreWindow.encodeToBase64(serializedDataStringJson)

    request = new ServerRequest(ServerRequest.REQUEST_TYPE_TRANSFORMATION_STORE, {
      key : key
      transformation_base64 : serializedDataStringBase64
    })


  ###
  Removes the transformation from the server-side storage

  param key the key used for the key-value store for the transformations
  ###
  removeServerSide: (key) ->
    request = new ServerRequest(ServerRequest.REQUEST_TYPE_TRANSFORMATION_STORE, { key : key })
