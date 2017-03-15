###
A console-window on the dashboard

param options =
  title                         # Name of the window (displayed in the titlebar)
  windowAutoscroll              # Whether the window should automatically scroll down after appending text to it
###
class ConsoleWindow extends Window
  @DEFAULT_TITLE = "Konsole"


  constructor: (@options = {}) ->
    # Set default parameters
    unless @options.title?
      @options.title = ConsoleWindow.DEFAULT_TITLE

    @options.id = "console_window"
    @options.offset = false
    @options.classes = ["consolewindow"]
    @options.content = new ConsoleContent(this, @options)

    super(@options)


  ###
  Appends a text to the console-textarea.

  This is a proxy to the ConsoleContent

  param text
  ###
  append: (text = "") ->
    @options.content.append(text)


  ###
  Overwrites the function from Window
  ###
  create: ->
    super()

    @nodeButtonClear = $("<span class='button'>&#9666;</span>")
    @nodeButtons.prepend(@nodeButtonClear)
    @enableClear()


  ###
  Makes the window clearable (clearing the content)
  ###
  enableClear: ->
    @nodeButtonClear.click( => @eventClickButtonClear() )


  ###
  Executed when the user wants to clear the window
  ###
  eventClickButtonClear: ->
    @options.content.clear()
