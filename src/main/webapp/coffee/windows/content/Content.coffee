###
Wraps the content to be displayed inside a Window

param window    # The window which belongs to this content
param options
  html          # The html to be used for the content
###
class Content
  @DEFAULT_HTML = ""

  constructor: (@window, @options = {}) ->
    @create()


  ###
  Creates the nodeContent
  ###
  create: ->
    if @options.html?
      @nodeContent = $(@options.html)
    else
      @nodeContent = $(Content.DEFAULT_HTML)
