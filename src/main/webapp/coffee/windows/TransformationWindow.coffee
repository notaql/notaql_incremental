###
A transformation-window on the dashboard. Belongs to a Transformation.

param options
###
class TransformationWindow extends Window
  @DEFAULT_TITLE = "Transformation"


  constructor: (@transformation, @options = {}) ->
    # Set default parameters
    unless @options.title?
      @options.title = @generateTitle()

    @options.classes = ["transformationwindow"]
    @options.offset = @transformation.nodeLine.offset()
    @options.content = new TransformationContent(@transformation, this, @options)

    super(@options)


  ###
  Triggered when one of the attached engines changed.
  ###
  eventEngineChanged: ->
      @setTitle(@generateTitle())
      @options.content.setScriptEngine(@transformation.generateScriptHeader())


  ###
  Generates the title of the window
  ###
  generateTitle: ->
    return "#{@transformation.engineIn.type?.typeName} -> #{@transformation.engineOut.type?.typeName}"


  ###
  Overwrites the function from Window
  ###
  close: ->
    super()
    @transformation.eventFocusLost()
