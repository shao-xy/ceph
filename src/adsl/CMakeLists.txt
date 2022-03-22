add_subdirectory(util)

add_library(adsl STATIC
	dout_wrapper.cc
)
target_link_libraries(adsl adsl-util "${TORCH_LIBRARIES}")
set_property(TARGET adsl PROPERTY CXX_STANDARD 14)
