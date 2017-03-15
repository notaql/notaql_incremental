###
Wraps the content to be displayed inside a StoreWindow

param window                    # The window which belongs to this content
param options
###
class StoreContent extends SwitchableContent
  @VIEWS = {
    base64 : "Base64",
    json : "Json"
  }


  constructor: (@window, @options = {}) ->
    @options.readonly = true
    
    super(@window, StoreContent.VIEWS, @options)


  ###
  Returns the initial data to be displayed
  ###
  getInitialData: ->
    return @window.base64


  ###
  Triggered when a different view was selected
  ###
  eventSelectedViewChanged: ->
      super()

      switch @currentView
        when @views.base64 then @nodeData.val(@window.base64)
        when @views.json then @nodeData.val(@window.json)
        else @nodeData.val("Unbekannte Auswahl: '#{@currentView}'")
