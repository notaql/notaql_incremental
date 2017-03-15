###
A line which is for example used for representing transformations

param options
###
class Line
  @DEFAULT_PARENT = "#svg"


  constructor: (@options = {}) ->
    # Set default parameters
    unless @options.parent?
      @options.parent = $(Line.DEFAULT_PARENT);

    # Create the line
    @nodeLine = $(document.createElementNS("http://www.w3.org/2000/svg", "line"));
    @options.parent.append(@nodeLine)


  ###
  Redraws the transformation line.
  ###
  redraw: (x1 = 0, y1 = 0, x2 = 0, y2 = 0) ->
    @nodeLine.attr({
       "x1" : x1
       "y1" : y1
       "x2" : x2
       "y2" : y2
    })


  ###
  Removes the line from the dashboard
  ###
  close: ->
    @nodeLine.remove()
