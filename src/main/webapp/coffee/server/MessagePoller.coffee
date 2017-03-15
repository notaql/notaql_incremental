###
The message poller. Retrieves messages from the server.

param options =
  pollingIntervallMillis    # The Intervall between polling-attemps from the server in milliseconds
###
class MessagePoller
  @DEFAULT_POLLING_INTERVALL_MILLIS = 10000
  @MESSAGE_TYPE_CONSOLE_APPEND = "console_append"

  constructor: (@options = {}) ->
    # Set default parameters
    unless @options.pollingIntervallMillis?
      @options.pollingIntervallMillis = MessagePoller.DEFAULT_POLLING_INTERVALL_MILLIS

    # Run the message-poller
    @run()

  ###
  Polls the server, waits some time and then calls itself again.
  ###
  run: ->
    @poll()

    setTimeout(=>
      @run()
    , @options.pollingIntervallMillis)


  ###
  Polls the messages from the server
  ###
  poll: ->
    pollingRequest = new ServerRequest(MessagePoller.MESSAGE_TYPE_CONSOLE_APPEND, null, { url : POLLING_URL })
    pollingRequest.addDoneFunction((returnJson) => @processResponse(returnJson))


  ###
  Processes the response-json from the server

  param returnJson
  ###
  processResponse: (returnJson) ->
    if returnJson?.messages?
      for message in returnJson.messages
        switch message.message_type
          when MessagePoller.MESSAGE_TYPE_CONSOLE_APPEND then @processConsoleAppend(message)
          else alert("Invalid Message received from the server #{JSON.stringify(message)}")


  ###
  Processes a message which is supposed to be shown in the consle
  ###
  processConsoleAppend: (messageObject) ->
    # First check if a console object was passed
    if Main.console
      Main.console.print(messageObject.text)
