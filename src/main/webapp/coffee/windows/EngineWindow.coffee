###
A console-window on the dashboard

param options =
  title                         # Name of the window (displayed in the titlebar)
  windowAutoscroll              # Whether the window should automatically scroll down after appending text to it
  {arguments}                   # The initial arguments of the server (e.g. "csv_path")
###
class EngineWindow extends Window
  @DEFAULT_TITLE = "Neue Engine"

  @lastConnectorClicked = null

  constructor: (@options = {}) ->
    # Set default parameters
    unless @options.title?
      @options.title = EngineWindow.DEFAULT_TITLE

    @options.classes = ["enginewindow"]
    @options.optimalSize = {
      width : true
      height : true
    }

    # Create the buttons and points for connecting engines with transformations
    @nodeLeftConnectionPoint = $("<span class='transformationConnectionPoint transformationConnectionPointLeft'></span>")
    @nodeRightConnectionPoint = $("<span class='transformationConnectionPoint transformationConnectionPointRight'></span>")
    @nodeLeftConnector = $("<span class='transformationConnector transformationConnectorLeft'>&#8226;</span>")
    @nodeRightConnector = $("<span class='transformationConnector transformationConnectorRight'>&#8226;</span>")

    unless @options.nodesAdditional?
      @options.nodesAdditional = []
    @options.nodesAdditional = @options.nodesAdditional.concat([@nodeLeftConnectionPoint, @nodeRightConnectionPoint, @nodeLeftConnector, @nodeRightConnector])

    # Register events
    @nodeLeftConnector.click( => @eventLeftConnectorClicked())
    @nodeRightConnector.click( => @eventRightConnectorClicked())

    super(@options)

    # Update the content. Has to be called after super().
    @setContent(new EngineContent(this, @options))


  ###
  Along with the information which connector was clicked last a new transformation
  is created (if it will be valid)
  ###
  @createNewTransformation: (clickEvent) ->
    # Check if this is the fist or the second click
    unless @lastConnectorClicked?
      # First click
      clickEvent.connector.addClass("transformationConnectorClicked")
      @lastConnectorClicked  = clickEvent

    else
      # Second click
      @lastConnectorClicked.connector.removeClass("transformationConnectorClicked")

      try
        # Sort the clicks first (the right connector belongs to the out-engine), then establish the connection
        if @lastConnectorClicked.isRightConnector and not clickEvent.isRightConnector
          inEngine = @lastConnectorClicked.engine
          outEngine = clickEvent.engine
        else if clickEvent.isRightConnector and not @lastConnectorClicked.isRightConnector
          inEngine = clickEvent.engine
          outEngine = @lastConnectorClicked.engine
        else
          Main.console.error("Es muss ein linker und ein rechter Connector genutzt werden")
          throw "Invalid Connectors"

        # Try to create the new transformation.
        # This will fail if it is not valid
        try new Transformation(inEngine, outEngine)

      @lastConnectorClicked = null


  ###
  Triggered when the engine-type was changed to another one.
  ###
  eventEngineTypeChanged: ->
    @setTitle(@type.typeName)
