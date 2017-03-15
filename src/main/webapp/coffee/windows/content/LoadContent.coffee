###
Wraps the content to be displayed inside a LoadWindow

param window                    # The window which belongs to this content
param options
###
class LoadContent extends SwitchableContent
  constructor: (@window, @options = {}) ->
    @options.readonly = false

    super(@window, StoreContent.VIEWS, @options)


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    super()

    # Create the nodes
    @nodeButtonLoad = $("<button type='button'>Laden</button>")

    # Put everything together
    @nodeFieldsetOptions.append(@nodeButtonLoad)

    # Enable events
    @nodeButtonLoad.click( => @window.callbackLoad())


  ###
  Returns the initial data to be displayed
  ###
  getInitialData: ->
    if LoadWindow.getLocalStorage()?
      return LoadWindow.getLocalStorage()
    else
      return ""


  ###
  Triggered when the display type was changed
  ###
  eventSelectedViewChanged: ->
      super()

      switch @currentView
        when @views.base64 then @changeToBase64()
        when @views.json then @changeToJson()
        else @nodeData.val("Unbekannte Auswahl: '#{@currentView}'")


  ###
  Changes the currently displayed format to base64.
  Therefor the current input is converted. If this fails the previous format will be selected.
  ###
  changeToBase64: ->
    jsonPretty = @nodeData.val()

    if jsonPretty.length isnt 0
      try
        json = JSON.stringify(LoadWindow.decodeJson(jsonPretty))
        @nodeData.val(StoreWindow.encodeToBase64(json))
      catch error
        @selectPreviousView()
        Main.console.error("JSON-Eingabe ist fehlerhaft (#{error})")

    else
      @nodeData.val("")


  ###
  Changes the currently displayed format to json
  Therefor the current input is converted. If this fails the previous format will be selected.
  ###
  changeToJson: ->
    base64 = @nodeData.val()

    if base64.length isnt 0
      try
        jsonPretty = JSON.stringify(LoadWindow.decodeBase64(base64), null, 1)
        @nodeData.val(jsonPretty)
      catch error
        @selectPreviousView()
        Main.console.error("Base64-Eingabe ist fehlerhaft (#{error})")
    else
      @nodeData.val("")
