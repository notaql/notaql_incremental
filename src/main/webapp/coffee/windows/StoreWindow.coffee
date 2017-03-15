###
A window on the dashboard used for displaying the stored data.

param base64
param json
param options
###
class StoreWindow extends Window
  @LOCALSTORAGE_KEY = "storedtransformation"
  @DEFAULT_TITLE = "Gespeichert!"


  constructor: (@base64, @json, @options = {}) ->
    # Set default parameters
    unless @options.title?
      @options.title = StoreWindow.DEFAULT_TITLE

    @options.classes = ["storewindow", "storeloadwindow"]
    @options.content = new StoreContent(this, @options)

    super(@options)


  ###
  Sets the data which is stored in the local storage

  param base64 encoded string
  ###
  @setLocalStorage: (base64) ->
    localStorage.setItem(StoreWindow.LOCALSTORAGE_KEY, base64)


  ###
  Encodes a jsonString to base64

  param jsonString
  return base64String
  ###
  @encodeToBase64: (jsonString) ->
    return $.base64.btoa($.trim(jsonString), true)
