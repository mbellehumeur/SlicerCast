#-----------------------------------------------------------------------------
set(MODULE_NAME CastInterface)

#-----------------------------------------------------------------------------
set(MODULE_PYTHON_SCRIPTS
  CastInterface.py
  Lib/CastHub.py
  Lib/CastImageDisplayClient.py
  Lib/CastResourceServers.py
  Lib/cast_client.py
  Lib/cast_provider_runtime.py
  Lib/image_display_client_hub.py
  Lib/resource_server_hub.py
  Lib/__init__.py
  )

#-----------------------------------------------------------------------------
slicerMacroBuildScriptedModule(
  NAME ${MODULE_NAME}
  SCRIPTS ${MODULE_PYTHON_SCRIPTS}
  RESOURCES Resources
  WITH_GENERIC_TESTS
  )

#-----------------------------------------------------------------------------
if(BUILD_TESTING)

  slicer_add_python_unittest(SCRIPT CastInterface.py)

  add_subdirectory(Testing)
endif()
