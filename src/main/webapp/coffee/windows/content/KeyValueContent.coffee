###
Content on a key-value basis

param window                    # The window which belongs to this content
param options
###
class KeyValueContent extends Content
  constructor: (@window, @options = {}) ->
    super(@window, @options)


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    # Create the nodes
    @nodeContent = $("<div class='content'></div>")
    @nodeFieldsetOptions = $("<fieldset><legend>Aktionen</legend></fieldset>")
    @nodeButtonStore = $("<button type='button'>Speichern</button>")
    @nodeButtonReload = $("<button type='button'>Neu laden</button>")
    @nodeFieldsetKeyValue = $("<fieldset><legend>Daten</legend></fieldset>")
    @nodeFieldsetKeyValueTable = $("<table style='border:none;'></table>")

    # Put everything together
    @nodeContent.append(@nodeFieldsetOptions)
    @nodeFieldsetOptions.append(@nodeButtonStore)
    @nodeFieldsetOptions.append(@nodeButtonReload)
    @nodeContent.append(@nodeFieldsetKeyValue)
    @nodeFieldsetKeyValue.append(@nodeFieldsetKeyValueTable)

    # Add inputs
    @createKeyValueInputs()

    # Enable events
    @nodeButtonStore.click( => @eventButtonPressedStore())
    @nodeButtonReload.click( => @eventButtonPressedLoad())


  ###
  Clears all key-value rows from the content

  This is a workaround - it didn't seem to work in another way
  ###
  clearKeyValues: ->
    for element in @nodeFieldsetKeyValue.children()
      element.remove()
    @inputs = {}


  ###
  Reloads the key-values from the window and ats them to the content.
  ###
  createKeyValueInputs: ->
    @inputs = {}
    for key, value of @window.getKeyValues()
      @inputs[key] = $("<input type='text' name='#{key}' placeholder='#{key}' value='#{value}'>")
      inputcontainer = $("<tr></tr>")
      inputcontainer.append($("<td><label for='#{key}'>#{key}</label></td>"))
      inputcontainer.append($("<td></td>").append(@inputs[key]))
      @nodeFieldsetKeyValue.append(inputcontainer)


  ###
  Triggered when the user wants to load the data
  ###
  eventButtonPressedLoad: ->
    @clearKeyValues()

    # Just enough timeout so the user sees a change
    setTimeout(=>
      @window.requestKeyValues()
    , 50)


  ###
  Triggered when the user wants to store the data
  ###
  eventButtonPressedStore: ->
    if not @inputs?
      @window.setKeyValues({})
    else
      keyValues = {}

      for key, input of @inputs
        keyValues[key] = input.val()

      @window.setKeyValues(keyValues)
