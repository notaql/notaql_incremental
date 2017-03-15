###
Wraps the execution of a notaql-script.

param transformation    # The notaql-transformation
param options =
  callbackFunction      # The function which will be executed after the execution finishes
  forecast              # If the user wants just a forecast of the runtime
  advisor               # If the user wants the system to take control about the execution mode
  resetAdvisor          # To reset the advisor used for "automated"
###
class NotaqlExecution
  constructor: (@transformation, @options={}) ->
    # Set default parameters
    unless @options.callbackFunction
      @options.callbackFunction = ->

    unless @options.forecast?
      @options.forecast = false

    unless @options.resetAdvisor?
      @options.resetAdvisor = false

    unless @options.advisor?
      @options.advisor = false

    @execute()


  ###
  Executes the script

  param callbackFunction is executed after the execution finishes
  ###
  execute: ->
    requestBody = @transformation.serialize()

    if @options.forecast
      requestBody.forecast = true

    if @options.advisor
      requestBody.advisor = true

    if @options.resetAdvisor
      requestBody.resetAdvisor = true

    @request = new ServerRequest(ServerRequest.REQUEST_TYPE_TRANSFORMATION_PROCESSOR, requestBody)
    @request.addDoneFunction( (returnJson) => @options.callbackFunction(returnJson) )
