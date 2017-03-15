###
A proxy to a console which displays messages

param options =
  isHavingWindow                # Whether the console should also have a window on the dashboard.
  isLoggingToBrowserConsole     # Whether the console should also log to the browser-console (e.g. Firebug)
  title                         # Name of the window (displayed in the titlebar)
  windowAutoscroll              # Whether the window should automatically scroll down after appending text to it
  isShowingDebugMessages        # If debug-messages shall be displayed
  isShowingWarningsWithAltert   # If warning-messages shall be displayed with alert()
###
class Console
  @DEFAULT_IS_HAVING_CONSOLE_WINDOW = true
  @DEFAULT_IS_LOGGING_TO_BROWSER_CONSOLE = true
  @DEFAULT_IS_SHOWING_DEBUG_MESSAGES = false
  @DEFAULT_IS_SHOWING_WARNINGS_WITH_ALTERT = true

  constructor: (@options = {}) ->
      # Set default parameters
      unless @options.isHavingWindow?
        @options.isHavingWindow = Console.DEFAULT_IS_HAVING_CONSOLE_WINDOW

      unless @options.isLoggingToBrowserConsole?
        @options.isLoggingToBrowserConsole = Console.DEFAULT_IS_LOGGING_TO_BROWSER_CONSOLE

      unless @options.isShowingDebugMessages?
        @options.isShowingDebugMessages = Console.DEFAULT_IS_SHOWING_DEBUG_MESSAGES

      unless @options.isShowingWarningsWithAltert?
        @options.isShowingWarningsWithAltert = Console.DEFAULT_IS_SHOWING_WARNINGS_WITH_ALTERT

      # Check if it is even possible to log to the browser console
      unless window.console
        @options.isLoggingToBrowserConsole = false

      # Create the window
      if @options.isHavingWindow
        @window = new ConsoleWindow(@options)


  ###
  Logs a text to the console

  param text
  ###
  print: (text = "") ->
    # Trim whitespace
    text = $.trim(text)

    # Print the text
    if @options.isHavingWindow
      @window.append(text)

    if @options.isLoggingToBrowserConsole
      console.log(text)


  ###
  Prints a debug message

  param text
  ###
  debug: (text) ->
    if @options.isShowingDebugMessages
      @print("[DEBUG] #{text}")


  ###
  Prints a error message

  param text
  ###
  error: (text) ->
    @print("[FEHLER] #{text}")

    if @options.isShowingWarningsWithAltert
      alert("Fehler: #{text}")
