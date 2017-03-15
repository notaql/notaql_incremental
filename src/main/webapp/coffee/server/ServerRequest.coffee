###
An asynchronous request to the server.
The request is automatically encoded and decoded with base64 to avoid strange errors.

param requestType       # The identifier with which the server identifiers the correct handler
param requestBody       # An object which will be sent to the server in the body of the request (e.g. the notaql-script)
param options
  url                   # To specify a url for the request
###
class ServerRequest
  @DEFAULT_URL = REQUEST_URL

  @REQUEST_TYPE_GET_ENGINES = "get_all_engines"
  @REQUEST_TYPE_TRANSFORMATION_PROCESSOR = "transformation_processor"
  @REQUEST_TYPE_CONFIGURATION_GET = "get_configuration"
  @REQUEST_TYPE_CONFIGURATION_SET = "set_configuration"
  @REQUEST_TYPE_KILL_ALL = "kill_all"
  @REQUEST_TYPE_KILL = "kill"
  @REQUEST_TYPE_TRANSFORMATION_STORE = "transformation_store"
  @REQUEST_TYPE_TRANSFORMATION_LOAD = "transformation_load"

  @DATAURI_JSON = "data:application/json;base64,"


  constructor: (@type, @body = {}, @options = {}) ->
    # Set default parameters
    unless @options.url?
      @options.url = ServerRequest.DEFAULT_URL

    # Send the request to the server
    @requestObject =
      request_type : @type
      request : @body

    @requestObjectString = encodeURIComponent($.base64.btoa(JSON.stringify(@requestObject), true))

    @request = $.ajax({
      url : @options.url
      method : "POST"
      data : "jsonBase64=#{@requestObjectString}"
    })


  ###
  Adds a function to the request which will be executed after completion.
  The functions will be executed even if the request fails.

  This function will decode the received base64 and also will try to convert the response into a java-object.
  If the conversion fails the text of the response will be passed to func

  param func The function to call with the response (or null) as parameter.
  ###
  addDoneFunction: (func) ->
    #@request.always(=> func)
    @request.always( (response) ->
      decodedResponse = null

      if response.responseText?
        decodedResponse = decodeURIComponent(response.responseText)

        # Check if this is base64 encoded json
        if (decodedResponse.lastIndexOf(ServerRequest.DATAURI_JSON, 0)) is 0
          try
            decodedResponse = $.base64.atob(decodedResponse.substring(ServerRequest.DATAURI_JSON.length), true)
          catch error
            Main.console.debug("JS-Fehler 64 @ ServerRequest.addDoneFunction(): #{ error}")

          try
            decodedResponse = JSON.parse(decodedResponse)

            if decodedResponse.response? and decodedResponse.response isnt "ok"
              Main.console.debug(JSON.stringify(decodedResponse, null, 1))
          catch error
            Main.console.debug("JS-Fehler 72 @ ServerRequest.addDoneFunction(): '#{ error}' for '#{decodedResponse}'")

      func(decodedResponse)
    )
