###
Wraps the content to be displayed inside a EngineWindow

param engine                    # The EngineWindow which belongs to this content
param options =
  server                        # Contains informations from the server
  {arguments}                   # The initial arguments of the server (e.g. "csv_path")
###
class EngineContent extends Content
  constructor: (@engine, @options = {}) ->
    super(@engine, @options)

    if @engine.type?
      @eventEngineTypeChanged()

      if @options.arguments?
        for argument, value of @options.arguments
          try
            if argument.substr(0,8) is "boolean_"
              @engineSpecificArguments[argument].prop("indeterminate", false)
              @engineSpecificArguments[argument].prop("checked", value)
            else
              @engineSpecificArguments[argument].val(value)
          catch error
            Main.console.error("Kann Parameter '#{argument}' nicht setzen (#{error})")


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    # Create the nodes
    @nodeContent = $("<div></div>")
    @nodeForm = $("<form action='#'></form>")
    @nodeSelectEngineType = $("<select name='engine_typename' class='engineselection'></select>")
    @nodeFieldsetEngineGeneric = $("<fieldset><legend>Engine</legend></fieldset>")
    @nodeFieldsetEngineSpecific = $("<fieldset class='enginespecific'></fieldset>")

    # Put everything together
    @nodeFieldsetEngineGeneric.append(@nodeSelectEngineType)
    @nodeForm.append(@nodeFieldsetEngineGeneric)
    @nodeContent.append(@nodeForm)

    # Insert the engines
    # If no type is currently set (which would happen if a type is passed to the Engine-constructor) a disabled one will be set as selected
    @nodeSelectEngineType.append($("<option value=''#{unless @engine.type? then ' selected' else ''} disabled>Bitte ausw&auml;hlen</option>"))
    for engine in @options.server.getEngines()
      isEngineSelected = @engine.type? and engine is @engine.type # Check if this engine was set in the Engine-constructor

      nodeEngine = $("<option value='#{engine.typeName}'#{if isEngineSelected then ' selected' else ''}>#{engine.typeName}</option>")
      @nodeSelectEngineType.append(nodeEngine)

    # Enable events for the select menu
    @nodeSelectEngineType.change( => @eventEngineTypeChanged())


  ###
  Triggered when any engine was selected (maybe even the same as it is at the moment)
  ###
  eventEngineTypeChanged: ->
    selectedEngine = @options.server.getEngine(@nodeSelectEngineType.val())

    if not @engineSpecificArguments? or @engine.type isnt selectedEngine
      @engine.type = selectedEngine
      @updateContent()
      @engine.eventEngineTypeChanged()


  ###
  Triggered when an engine argument was changed
  ###
  eventEngineArgumentChanged: ->
    @engine.eventEngineArgumentChanged()


  ###
  Triggered when a *boolean* engine argument was changed

  Ensures that the indeterminate state is still available after a click.

  param event the event data
  ###
  eventBooleanEngineArgumentChanged: (event) ->
    nodeInput = $(event.target)

    # Ensure indeterminate
    # Source: css-tricks.com/indeterminate-checkboxes/
    switch nodeInput.data("checked")
      when 0
        # unchecked -> indeterminate
        nodeInput.data("checked", 1)
        nodeInput.prop("indeterminate", true)

      when 1
        # indeterminate -> checked
        nodeInput.data("checked", 2)
        nodeInput.prop("indeterminate", false)
        nodeInput.prop("checked", true)

      else
        # checked -> unchecked
        nodeInput.data("checked", 0)
        nodeInput.prop("indeterminate", false)
        nodeInput.prop("checked", false)

    @eventEngineArgumentChanged()


  ###
  Updates engine-specific stuff in the content.

  Called by Engine -> EngineWindow -> EngineContent
  ###
  updateContent: ->
    if @nodeForm.find("fieldset.enginespecific").length && @engine.type.arguments.length == 0
      # Fieldset exists but engine has no arguments
      @nodeForm.remove(@nodeFieldsetEngineSpecific)
    else if not @nodeForm.find("fieldset.enginespecific").length
      # Fieldset does not exist
      @nodeForm.append(@nodeFieldsetEngineSpecific)
    else
      # Fieldset exists
      @nodeFieldsetEngineSpecific.empty()

    @nodeFieldsetEngineSpecific.append($("<legend>#{@engine.type.typeName}</legend>"))

    @engineSpecificArguments = {}
    for argument in @engine.type.arguments
      inputPlaceholder = "#{argument}#{if @engine.type.isArgumentRequired(argument) then ' (*)' else ''}"
      if argument.substr(0,8) is "boolean_"
        @engineSpecificArguments[argument] = $("<input type='checkbox' name='#{argument}' value='#{argument}'>")

        # Initial state is null
        @engineSpecificArguments[argument].data("checked", 1)
        @engineSpecificArguments[argument].prop("indeterminate", true)

        @nodeFieldsetEngineSpecific.append($("<div class='inputcontainer'></div>").append($("<label></label>").append(@engineSpecificArguments[argument], "<span class='text'>#{inputPlaceholder.substr(8,inputPlaceholder.length)}</span>")))
        @engineSpecificArguments[argument].on("change", (event) => @eventBooleanEngineArgumentChanged(event))
      else
        @engineSpecificArguments[argument] = $("<input type='text' name='#{argument}' placeholder='#{inputPlaceholder}' class='engineparameter'>")
        @nodeFieldsetEngineSpecific.append($("<div class='inputcontainer'></div>").append(@engineSpecificArguments[argument]))
        @engineSpecificArguments[argument].bind("input", => @eventEngineArgumentChanged() )

    @engine.eventContentChanged()


  ###
  Triggered when the attached transformation is set to automated.

  Sets nearly everything to readonly

  param newState
  ###
  eventSetAutomated: (newState) ->
    for argument, nodeInput of @engineSpecificArguments
      if argument not in @engine.type.advisorArguments
        nodeInput.attr("readonly", newState)
    @nodeSelectEngineType.attr("disabled", newState)
