###
Base class for Contents which have differnt views to display the data (e.g. StoreContent or LoadContent)

param window          # The window which belongs to this content
param views           # The views which may be displayed (e.g. base64 and json)
param options
  readonly            # Whether or not the textarea shall be readonly
###
class SwitchableContent extends Content
  @DEFAULT_READONLY = true
  @DEFAULT_VIEWS = {
    default : "Standard"
  }


  constructor: (@window, @views = SwitchableContent.DEFAULT_VIEWS, @options = {}) ->
    # Set default parameters
    unless @options.readonly?
      @options.readonly = SwitchableContent.DEFAULT_READONLY

    # Object variables
    @previousView = null
    @currentView = null

    super(@window, @options)


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    # Create the nodes
    @nodeContent = $("<div class='content'></div>")
    @nodeFieldsetOptions = $("<fieldset><legend>Optionen</legend></fieldset>")
    @nodeSelectView = $("<select></select>")
    @nodeData = $("<textarea class='grow'#{if @options.readonly then ' readonly=\'1\'' else ''}>#{@getInitialData()}</textarea>")

    isFirst = true
    for key, value of @views
      @nodeSelectView.append( $("<option value='#{value}'#{if isFirst then ' selected' else ''}>#{value}</option>") )
      if isFirst
        @currentView = value
        isFirst = false

    # Put everything together
    @nodeContent.append(@nodeFieldsetOptions)
    @nodeFieldsetOptions.append(@nodeSelectView)
    @nodeContent.append(@nodeData)

    # Enable events
    @nodeSelectView.change( => @eventSelectedViewChanged())


  ###
  Returns the initial data to be displayed
  ###
  getInitialData: ->
    return ""


  ###
  Updates the state of the view-variables
  ###
  updateVariables: ->
    newView = @nodeSelectView.val()

    if @currentView isnt newView
      @previousView = @currentView
      @currentView = newView


  ###
  Selects the view which was previously selected (if any)
  ###
  selectPreviousView: ->
    if @previousView?
      @nodeSelectView.val(@previousView)
      @updateVariables()


  ###
  Triggered when a different view was selected
  ###
  eventSelectedViewChanged: ->
    @updateVariables()
