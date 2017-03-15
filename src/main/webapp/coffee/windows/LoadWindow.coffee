###
A window on the dashboard used for entering serialized data.

param callbackFunctionLoad    # Callback function which is called with the data the user wants to load
param options
###
class LoadWindow extends Window
  @LOCALSTORAGE_KEY = StoreWindow.LOCALSTORAGE_KEY
  @DEFAULT_TITLE = "Dashboard laden"


  constructor: (@callbackFunctionLoad, @options = {}) ->
    # Set default parameters
    unless @options.title?
      @options.title = LoadWindow.DEFAULT_TITLE

    @options.classes = ["loadwindow", "storeloadwindow"]
    @options.content = new LoadContent(this, @options)

    super(@options)


  ###
  Calls the callback function which loads the data
  ###
  callbackLoad: ->
    data = @options.content.nodeData.val()
    format = @options.content.currentView
    decodedData = null

    if format is @options.content.views.base64
      try
        decodedData = LoadWindow.decodeBase64(data)
        @callbackFunctionLoad(decodedData)
      catch error
        Main.console.error("Base64-Eingabe ist fehlerhaft (#{error})")
    else if format is @options.content.views.json
      try
        decodedData = LoadWindow.decodeJson(data)
        @callbackFunctionLoad(decodedData)
      catch error
        Main.console.error("JSON-Eingabe ist fehlerhaft (#{error})")
    else
      Main.console.error("Unbekanntes Format: '#{callbackData.format}'")

    if decodedData?
      @close()


  ###
  Gets the data which is stored in the local storage

  return base64 encoded string or null
  ###
  @getLocalStorage: ->
    return $.trim(localStorage.getItem(LoadWindow.LOCALSTORAGE_KEY))


  ###
  Decodes a base64 encoded json string into an object

  param base64
  return object or empty string
  ###
  @decodeBase64: (base64) ->
    if not base64? or base64.length is 0
      return ""
    else
      return JSON.parse($.base64.atob($.trim(base64), true))


  ###
  Decodes a json string into an object

  param jsonString
  return object or empty string
  ###
  @decodeJson: (jsonString) ->
    if not jsonString? or jsonString.length is 0
      return ""
    else
      return JSON.parse($.trim(jsonString))
