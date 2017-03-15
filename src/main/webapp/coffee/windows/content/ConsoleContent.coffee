###
Wraps the content to be displayed inside a ConsoleWindow

param window                    # The window which belongs to this content
param options =
  windowAutoscroll              # Whether the window should automatically scroll down after appending text to it
###
class ConsoleContent extends Content
  @DEFAULT_WINDOW_AUTOSCROLL = true


  constructor: (@window, @options = {}) ->
    # Set default parameters
    unless @options.windowAutoscroll?
      @options.windowAutoscroll = ConsoleContent.DEFAULT_WINDOW_AUTOSCROLL

    super(@window, @options)


  ###
  Creates the initial nodeContent (the selection for the engine-type)

  Overrides the function from super()
  ###
  create: ->
    # Create the nodes
    @nodeContent = $("<textarea class='grow transparent' readonly='1'></textarea>")


  ###
  Appends a text to the textarea

  param text
  ###
  append: (text = "") ->
    @nodeContent.val(@nodeContent.val() + text + "\n")

    if @options.windowAutoscroll
      @scrollDown()


  ###
  Scrolls down the textarea to the end of the text
  ###
  scrollDown: ->
    @nodeContent.scrollTop(@nodeContent[0].scrollHeight - @nodeContent.height())


  ###
  Clears the content
  ###
  clear: ->
    @nodeContent.val("")
    @append("Konsole geleert")
