###
A window on the dashboard

param options =
  parent          # Parent for the new window-node.
  id              # ID for the window.
  [classes]       # Additional classes for the window
  title           # Title for the window.
  content         # Content-node for the window.
  hidden          # Whether or not the window will be spawned with css("display", "hidden")
  nodesAdditional # Additional DOM-nodes to append to the window (e.g. transformation-connectors).
  optimalSize     # Resize the window after creation so it fits the content perfectly
  offset          # The window will be spawned at the given offset (see jQuery.offset())
###
class Window
  @DEFAULT_PARENT = "body"
  @DEFAULT_TITLE = "Neues Fenster"
  @DEFAULT_HIDDEN = false
  @DEFAULT_OPTIMAL_SIZE = false

  @nextWindowId = 0
  @currentMaxZIndexWindow = null
  @minWindowOffset = 5
  @maxWindowOffset = 80
  @nextWindowOffsetStep = 11
  @nextWindowOffset = Window.minWindowOffset


  constructor: (@options = {}) ->
    # Set default parameters
    unless @options.parent?
      @options.parent = $(Window.DEFAULT_PARENT)

    unless @options.title?
      @options.title = Window.DEFAULT_TITLE

    unless @options.hidden?
      @options.hidden = Window.DEFAULT_HIDDEN

    unless @options.nodesAdditional?
      @options.nodesAdditional = []

    unless @options.id?
      @options.id = "window#{Window.nextWindowId++}";

    unless @options.content?
      @options.content = new Content()

    unless @options.optimalSize?
      @options.optimalSize = Window.DEFAULT_OPTIMAL_SIZE

    # Create the window
    @create()

    if @options.optimalSize
      @autoResize(@options.optimalSize.width, @options.optimalSize.height)

    @moveToFront()


  ###
  Create the window inside the parent
  ###
  create: ->
    # Create the nodes
    @nodeTitle = $("<span class='title'>#{@options.title}</span>")
    @nodeButtonMinimize = $("<span class='button buttonminimize'>_</span>")
    @nodeButtonMaximize = $("<span class='button buttonmaximize'>&#9723;</span>")
    @nodeButtonClose = $("<span class='buttonclose'>x</span>")
    @nodeButtons = $("<span class='buttons'></span>")
    @nodeTitlebar = $("<div class='titlebar'></div>")
    @nodeContent = $("<div class='content'></div>")
    @nodeWindow = $("<div id='#{@options.id}' class='window'></div>")

    if @options.hidden
      @nodeWindow.css("display", "none")

    if @options.offset
      @nodeWindow.offset(@options.offset)
    else if not @options.offset?
      offset = "#{Window.nextWindowOffset}%";
      Window.nextWindowOffset = Window.nextWindowOffset + Window.nextWindowOffsetStep
      if Window.nextWindowOffset > Window.maxWindowOffset
        Window.nextWindowOffset = Window.minWindowOffset + (Window.nextWindowOffset - Window.maxWindowOffset)

      @nodeWindow.css({
        top : offset
        left : offset
      })



    if @options.classes?
      for _class in @options.classes
        @nodeWindow.addClass(_class)

    # Put everything together
    @nodeTitlebar.append(@nodeTitle)
    @nodeButtons.append(@nodeButtonMinimize)
    @nodeButtons.append(@nodeButtonMaximize)
    @nodeButtons.append(@nodeButtonClose)
    @nodeTitlebar.append(@nodeButtons)
    @nodeContent.append(@options.content.nodeContent)
    @nodeWindow.append(@nodeTitlebar)
    @nodeWindow.append(@nodeContent)

    for nodeAdditional in @options.nodesAdditional
      @nodeWindow.append(nodeAdditional)

    @options.parent.append(@nodeWindow)

    @enableWindowFeatures()


  ###
  Enables window features
  ###
  enableWindowFeatures: ->
    @enableDragable()
    @enableResizable()
    @enableMinimize()
    @enableMaximize()
    @enableClose()
    @enableClickWindow()


  ###
  Makes the window draggable
  ###
  enableDragable: ->
    @nodeWindow.draggable({
      start : => @eventDragged()
      drag : => @eventDragged()
      stop : => @eventDragged()
    })


  ###
  Executed at drag-events
  ###
  eventDragged: ->
    $("#svg").attr("height", "100%")

    # Hide the tooltip (if any)
    @repositionMessage()


  ###
  Makes the window draggable
  ###
  enableResizable: ->
    @nodeWindow.resizable({
      handles: "n, e, w, s"
      start : => @eventResized()
      resize : => @eventResized()
      stop : => @eventResized()
      containment : "document"
    })


  ###
  Executed at resize-events
  ###
  eventResized: ->
    # Hide the tooltip (if any)
    @hideMessage()


  ###
  Makes the window minimizeable
  ###
  enableMinimize: ->
    @nodeButtonMinimize.click( => @eventClickButtonMinimize() )


  ###
  Executed when the user wants to change the minimize-state of a window
  ###
  eventClickButtonMinimize: ->
    if @getIsMinimized()
      @setIsMinimized(false)
    else
      @setIsMinimized(true)


  ###
  Makes the window maximizeable
  ###
  enableMaximize: ->
    @nodeButtonMaximize.click( => @eventClickButtonMaximize() )


  ###
  Executed when the user wants to change the maximize-state of a window
  ###
  eventClickButtonMaximize: ->
    if @getIsMaximized()
      @setIsMaximized(false)
    else
      @setIsMaximized(true)


  ###
  Makes the window closeable
  ###
  enableClose: ->
    @nodeButtonClose.click( => @eventClickButtonCloes() )


  ###
  Executed when the user wants to close the window
  ###
  eventClickButtonCloes: ->
    @close()


  ###
  Makes the windows clickable
  ###
  enableClickWindow: ->
    @nodeWindow.click( => @eventClickWindow() )


  ###
  Executed when the user wants to close the window
  ###
  eventClickWindow: ->
    @moveToFront()


  ###
  Changes the title for this window

  param newTitle
  ###
  setTitle: (newTitle = "") ->
    @nodeTitle.text(newTitle)


  ###
  Gets the display-state for this widnow

  return true if displayed, false otherwise
  ###
  getIsDisplayed: ->
    return @nodeWindow.css("display").toLowerCase() isnt "none"


  ###
  Changes the display-state for this window (hidden / displayed)

  param newState
  ###
  setIsDisplayed: (newState = true, animate = true) ->
    if @getIsDisplayed() and not newState
      # Hide the window
      if animate
        @nodeWindow.fadeOut("fast")
      else
        @nodeWindow.css("display", "none")
    else if not @getIsDisplayed() and newState
      # Show the window
      if animate
        @nodeWindow.fadeIn("fast")
      else
        @nodeWindow.css("display", "block")


  ###
  Gets the minimized-state for this window

  return true if minimized, false otherwise
  ###
  getIsMinimized: ->
    unless @stateIsMinimized?
      return false
    else
      return @stateIsMinimized


  ###
  Changes the minimized state for this window (minimized / un-minimized)

  param newState
  ###
  setIsMinimized: (newState = false, animate = true) ->
    # Check if the current state and the target states differ
    if @getIsMinimized() and not newState
      # Is minimized, shall be un-minimized
      @nodeContent.appendTo(@nodeWindow)

      if animate
        @nodeWindow.animate({
          height : @minimizePrevHeight
        }, =>
          if @options.optimalSize and @options.optimalSize.height then @nodeWindow.height("auto")
          @eventResized())
      else
        @nodeWindow.height(@minimizePrevHeight)
        @eventResized()

      @stateIsMinimized = newState

    else if not @getIsMinimized() and newState
      # Is not minimized, shall be minimized
      @minimizePrevHeight = @nodeWindow.height()

      if animate
        @nodeWindow.animate({
          height : @nodeWindow.css("min-height")
        }, =>
          @nodeContent.detach()
          @eventResized()
        )
      else
        @nodeWindow.height(@nodeWindow.css("min-height"))
        @nodeContent.detach()
        @eventResized()

      @stateIsMinimized = newState

    @eventResized()


  ###
  Gets the maximized-state for this window

  return true if maximized, false otherwise
  ###
  getIsMaximized: ->
    unless @stateIsMaximized?
      return false
    else
      return @stateIsMaximized


  ###
  Changes the maximized state for this window (maximized / un-maximized)

  param newState
  ###
  setIsMaximized: (newState = false) ->
    # Check if the current state and the target states differ
    if @getIsMaximized() and not newState
      # Is maximized, shall be un-maximized
      # Hide the window, change the settings, then fade it in.
      @setIsDisplayed(false, false)
      @setIsMinimized(@maximizePrev.wasMinimized, false)
      @minimizePrevHeight = @maximizePrev.minimizePrevHeight

      @nodeWindow.css({
        top : @maximizePrev.position.top
        left : @maximizePrev.position.left
        width : @maximizePrev.width
        height : @maximizePrev.height
      })

      @nodeWindow.removeClass("nottransparent")

      @setIsDisplayed(true, false)

      @stateIsMaximized = newState

    else if not @getIsMaximized() and newState
      # Is not maximized, shall be maximized
      @maximizePrev =
        position : @nodeWindow.position()
        width : @nodeWindow.width()
        height : @nodeWindow.height()
        wasMinimized : @getIsMinimized()
        minimizePrevHeight : @minimizePrevHeight

      # Hide the window, change the settings, then fade it in.
      # If the window was minimized we also have to maximize it in order to show the content
      @setIsDisplayed(false, false)
      @setIsMinimized(false, false)

      @nodeWindow.css({
        top : "2%"
        left : "2%"
        width : "96%"
        height : "96%"
      })

      @nodeWindow.addClass("nottransparent")

      @setIsDisplayed(true, false)

      @stateIsMaximized = newState

    @eventResized()


  ###
  Closes the window
  ###
  close: ->
    @nodeWindow.fadeOut("fast");


  ###
  Moves the window to the front of the dashboard (z-index increment)
  ###
  moveToFront: ->
    if Window.currentMaxZIndexWindow?
      nextZIndex = Window.currentMaxZIndexWindow.nodeWindow.zIndex() + 1
    else
      # Window.currentMaxZIndexWindow Is null
      nextZIndex = 1000

    @nodeWindow.zIndex(nextZIndex)

    Window.currentMaxZIndexWindow = this


  ###
  Resizes the window to perfectly fit the content

  param resizeWidth true if width shall be resized, false otherwise
  param resizeHeight true if height shall be resized, false otherwise
  ###
  autoResize: (resizeWidth = true, resizeHeight = true) ->
    if resizeWidth
      @nodeWindow.width("auto")

    if resizeHeight
      @nodeWindow.height("auto")

    @eventResized()


  ###
  Changes the content of this window

  param newContent Instance of class Content
  ###
  setContent: (newContent) ->
    @options.content = newContent
    @nodeContent.append(@options.content.nodeContent)

    @eventContentChanged()


  ###
  Executed when the content changes
  ###
  eventContentChanged: ->
    if @options.optimalSize
      @autoResize(@options.optimalSize.width, @options.optimalSize.height)


  ###
  Shows multiple messages below the window (e.g. a hint on how to optimize the query)

  param texts the texts to show (array)
  ###
  showMessages: (texts) ->
    @showMessage(texts.join("\n"))


  ###
  Shows a message below the window (e.g. a hint on how to optimize the query)

  param text the text to show
  ###
  showMessage: (text) ->
    @hideMessage()
    @nodeWindow.attr("title", text);
    @createTooltip()

    # Hide the tooltip after a while
    @tooltip = setTimeout(=>
      @hideMessage()
    , 10000)


  ###
  Creates the tooltip
  ###
  createTooltip: ->
    @nodeWindow.tooltip({
      show : false
      hide : { duration : 9999999 } # This high number effectivly disables the hiding of the tooltip until it gets destroyed
    });
    @nodeWindow.tooltip("open");


  ###
  Re-sets the position of the message (if any)
  ###
  repositionMessage: =>
    if @tooltip? and @tooltip
      @nodeWindow.tooltip("destroy");
      @createTooltip()


  ###
  Hides the message (if any)
  ###
  hideMessage: =>
    if @tooltip? and @tooltip
      @nodeWindow.tooltip("destroy");
      @nodeWindow.removeAttr("title");
      clearTimeout(@tooltip)
      @tooltip = false
