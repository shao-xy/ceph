add_subdirectory(util)

add_library(adsl STATIC
	main.cc
)
target_link_libraries(adsl adsl-util)
