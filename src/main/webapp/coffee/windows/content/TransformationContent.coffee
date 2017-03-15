###
Wraps the content to be displayed inside a TransformationWindow

param transformation            # The Transformation which belongs to this content
param window                    # The TransformationWindow which belongs to this content
param options
###
class TransformationContent extends Content
  @EXECUTION_SPAMMING_AFTER_MILLIS = 60000;

  constructor: (@transformation, @window, @options = {}) ->
    # Object variables
    @execution = null

    super(@window, @options)


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    # Create the nodes
    @nodeContent = $("<div class='content'></div>")
    @nodeFieldsetActions = $("<fieldset><legend>Aktionen</legend></fieldset>")
    @nodeButtonStart = $("<button type='button'>Start</button>")
    @nodeButtonStartAutomate = $("<button type='button'>Automatisieren</button>")
    @nodeButtonForecast = $("<button type='button'>Laufzeitprognose</button>")
    @nodeButtonDelete = $("<button type='button'>L&ouml;schen</button>")
    @nodeScriptEngines = $("<textarea rows='4' readonly='1' class='scriptengines'></textarea>")
    @nodeScript = $("<textarea rows='8' class='grow'></textarea>")

    @setScriptEngine()

    # Put everything together
    @nodeContent.append(@nodeFieldsetActions)
    @nodeFieldsetActions.append(@nodeButtonStart)
    @nodeFieldsetActions.append(@nodeButtonForecast)
    @nodeFieldsetActions.append(@nodeButtonDelete)
    @nodeFieldsetActions.append(@nodeButtonStartAutomate)
    @nodeContent.append(@nodeFieldsetScript)
    @nodeContent.append(@nodeScriptEngines)
    @nodeContent.append(@nodeScript)

    # Enable events
    #@nodeScriptEngines.click( => @nodeScript.focus())
    @nodeButtonStart.click( => @eventButtonPressedStartStop())
    @nodeButtonStartAutomate.click( => @eventButtonPressedStartAutomate())
    @nodeButtonForecast.click( => @eventButtonPressedForecast())
    @nodeButtonDelete.click( => @eventButtonPressedDelete())


  ###
  Sets the text in the engine-line textarea.

  param text the new engine lines
  ###
  setScriptEngine: ->
    @nodeScriptEngines.text(@transformation.generateScriptHeader())


  ###
  Sets the text in the big notaql-script textarea.

  param text the new script
  ###
  setScriptBody: (text) ->
    @nodeScript.text(text)


  ###
  Gets the text in the big notaql-script textarea.

  return text the script-body
  ###
  getScriptBody: ->
    return $.trim(@nodeScript.val())


  ###
  Triggered when the start/stop button is pressed
  ###
  eventButtonPressedStartStop: ->
    if @execution? and not @transformation.triggerTransformationId?
      Main.console.error("Ausfuehrung laeuft schon")

    else if @transformation.triggerTransformationId?
      @transformation.stopTriggerTransformation()

    else if @transformation.isValidExecution()
      Main.console.print("Transformation gestartet: #{@toString()}")
      @nodeButtonStart.text("Aktiv...")
      @startTimeMs = (new Date).getTime()

      @execution = new NotaqlExecution(@transformation, {
        callbackFunction : ( (returnJson) => @eventExecutionFinished(returnJson) )
        advisor : true if @automate?
      })

      if ("triggerbased" in @transformation.engineIn.type.arguments or "boolean_triggerbased" in @transformation.engineIn.type.arguments) and ((@startTimeMs - @finishTime) < TransformationContent.EXECUTION_SPAMMING_AFTER_MILLIS)
        @window.showMessage("Sie starten die Transformation sehr oft. Vermutlich ist eine triggerbasierte Ausfuehrung geeigneter.")


  ###
  Triggered when the automate button is pressed
  ###
  eventButtonPressedStartAutomate: ->
    if @execution? or @transformation.triggerTransformationId?
      Main.console.error("Die aktuelle Transformation muss zuerst abgeschlossen werden")

    else if @transformation.isValidExecution()
      if @automate? and @automate
        @nodeButtonStartAutomate.html("Wird deaktiviert ...")
        new NotaqlExecution(@transformation, {
          callbackFunction : ( (returnJson) =>
            if returnJson.response is "error"
              Main.console.error(returnJson.error_message)
            else if returnJson.response is "ok"
              Main.console.print("Transformation wieder normal: #{@toString()}")
              @automate = null
              @nodeButtonStartAutomate.html("Automatisieren")
              @nodeScript.attr("readonly", false)
              @transformation.engineIn.options.content.eventSetAutomated(false)
              @transformation.engineOut.options.content.eventSetAutomated(false)
            )
          resetAdvisor : true
        })

      else
        Main.console.print("Transformation automatisiert: #{@toString()}")
        @automate = true
        @nodeButtonStartAutomate.html("&#10003; Automatisiert")
        @nodeScript.attr("readonly", true)
        @transformation.engineIn.options.content.eventSetAutomated(true)
        @transformation.engineOut.options.content.eventSetAutomated(true)


  ###
  Triggered when the forecast button is pressed
  ###
  eventButtonPressedForecast: ->
    if @automate? and @automate
      Main.console.error("Laufzeitprognosen sind bei automatisierten Transformationen nicht moeglich")

    else if @transformation.isValidExecution()
      Main.console.print("Erstelle Laufzeitprognose: #{@toString()}")

      new NotaqlExecution(@transformation, {
        callbackFunction : ( (returnJson) =>
            if returnJson.response is "error"
              Main.console.error(returnJson.error_message)
            else if returnJson.response is "ok"
              if returnJson.map?.forecast?
                if returnJson.map.forecast is "exception"
                  Main.console.error(returnJson.map.error_message)
                else if returnJson.map.forecast is "ok"
                  runtimeMillis = returnJson.map.forecast_result.runtimeMillis
                  similarityPercent = Math.round(returnJson.map.forecast_result.similarity*100)
                  Main.console.print("Die zu '#{@toString()}' aehnlichsten Transformationen brauchten durchschnittlich #{runtimeMillis} ms (Aehnlichkeit: #{similarityPercent}%)")
                  @window.showMessage("Erwartete Laufzeit: #{runtimeMillis} ms (#{similarityPercent}%)")
          )
        forecast : true
      })


  ###
  Triggered when the delete button is pressed
  ###
  eventButtonPressedDelete: ->
    if @execution? or @transformation.triggerTransformationId? or @automate?
      Main.console.error("Die aktuelle Transformation muss zuerst abgeschlossen werden")

    else
      if confirm("Wirklich die Transformation loeschen?")
        if @automate? and @automate
          new NotaqlExecution(@transformation, { resetAdvisor : true })
        @transformation.close()


  ###
  Clears the currently running execution (only in the UI)
  ###
  clearExecution: ->
    @nodeButtonStart.text("Start")
    @execution = null
    @transformation.triggerTransformationId = null


  ###
  Triggered when the execution of the script is finished

  param returnJson the json-data returned by the NotaqlExecution
  ###
  eventExecutionFinished: (returnJson) ->
    @finishTime = (new Date).getTime()
    totalTimeMs = @finishTime - @startTimeMs
    Main.console.print("Ausfuehrung abgeschlossen: #{@toString()} (#{totalTimeMs} ms)")

    if returnJson.response is "error"
      Main.console.error(returnJson.error_message)

    else if returnJson.response is "ok"
      # Set new values which were piggybanked with the response
      if returnJson.map?.new_values?
        if returnJson.map.new_values.in_engine?
          for key, newvalue of returnJson.map.new_values.in_engine
            @transformation.engineIn.setArgument(key, newvalue)
        if returnJson.map.new_values.out_engine?
          for key, newvalue of returnJson.map.new_values.out_engine
            @transformation.engineOut.setArgument(key, newvalue)

      # If there are messages: display them
      if returnJson.map?.messages?
        if returnJson.map.messages.in_engine?
          @transformation.engineIn.showMessages(returnJson.map.messages.in_engine)
        if returnJson.map.messages.out_engine?
          @transformation.engineIn.showMessages(returnJson.map.messages.out_engine)

      # If this is a trigger based transformation: handle it
      if returnJson.map?.trigger?
        if returnJson.map.trigger is "exception"
          Main.console.error(returnJson.map.error_message)
        else if returnJson.map.trigger is "ok"
          @transformation.eventTriggerTransformationStarted(returnJson.map)

    unless @transformation.triggerTransformationId?
      @clearExecution()


  ###
  Erstellt eine Stringrepraesentation des Objekts

  return string
  ###
  toString: ->
    return @window.generateTitle()
