#!/bin/sh
cd ../js
coffee --watch --join dashboard.js --compile \
  ../coffee/server/ServerRequest.coffee \
  ../coffee/server/MessagePoller.coffee \
  ../coffee/server/NotaqlExecution.coffee \
  ../coffee/server/ServerEngine.coffee \
  ../coffee/server/Server.coffee \
  ../coffee/windows/content/Content.coffee \
  ../coffee/windows/content/ConsoleContent.coffee \
  ../coffee/windows/content/EngineContent.coffee \
  ../coffee/windows/content/KeyValueContent.coffee \
  ../coffee/windows/content/SwitchableContent.coffee \
  ../coffee/windows/content/StoreContent.coffee \
  ../coffee/windows/content/LoadContent.coffee \
  ../coffee/windows/content/TransformationContent.coffee \
  ../coffee/windows/Window.coffee \
  ../coffee/windows/ConfigurationWindow.coffee \
  ../coffee/windows/ConsoleWindow.coffee \
  ../coffee/windows/EngineWindow.coffee \
  ../coffee/windows/StoreWindow.coffee \
  ../coffee/windows/LoadWindow.coffee \
  ../coffee/windows/TransformationWindow.coffee \
  ../coffee/geometrical/Line.coffee \
  ../coffee/dashboard/Engine.coffee \
  ../coffee/dashboard/Transformation.coffee \
  ../coffee/Console.coffee \
  ../coffee/Main.coffee
