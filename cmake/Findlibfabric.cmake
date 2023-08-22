find_path(LIBFABRIC_INCLUDE_DIR NAMES rdma/fabric.h PATH_SUFFIXES include)
mark_as_advanced(LIBFABRIC_INCLUDE_DIR)

if(NOT LIBFABRIC_LIBRARY)
    find_library(LIBFABRIC_LIBRARY_RELEASE NAMES libfabric fabric
        PATH_SUFFIXES release
        NAMES_PER_DIR
    )
    mark_as_advanced(LIBFABRIC_LIBRARY_RELEASE)

    find_library(LIBFABRIC_LIBRARY_DEBUG NAMES libfabric fabric
        PATH_SUFFIXES debug
        NAMES_PER_DIR
    )
    mark_as_advanced(LIBFABRIC_LIBRARY_DEBUG)

    include(SelectLibraryConfigurations)
    select_library_configurations(LIBFABRIC)
endif()

find_package(PackageHandleStandardArgs)
find_package_handle_standard_args(libfabric
    REQUIRED_VARS LIBFABRIC_LIBRARY LIBFABRIC_INCLUDE_DIR
)

if(LIBFABRIC_FOUND)
    set(LIBFABRIC_LIBRARIES ${LIBFABRIC_LIBRARY})
    set(LIBFABRIC_INCLUDE_DIRS ${LIBFABRIC_INCLUDE_DIR})
    if(WIN32)
        list(APPEND LIBFABRIC_INCLUDE_DIRS ${LIBFABRIC_INCLUDE_DIR}/windows)
    endif()

    if(NOT TARGET libfabric::libfabric)
        if(LIBFABRIC_dll_release AND LIBFABRIC_dll_debug)
            add_library(libfabric::libfabric SHARED IMPORTED)
            set_property(TARGET libfabric::libfabric APPEND PROPERTY
                IMPORTED_CONFIGURATIONS DEBUG RELEASE)
            set_target_properties(libfabric::libfabric PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${LIBFABRIC_INCLUDE_DIRS}"
                IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                IMPORTED_IMPLIB_RELEASE "${LIBFABRIC_LIBRARY_RELEASE}"
                IMPORTED_IMPLIB_DEBUG "${LIBFABRIC_LIBRARY_DEBUG}"
                IMPORTED_LOCATION_RELEASE "${LIBFABRIC_dll_release}"
                IMPORTED_LOCATION_DEBUG "${LIBFABRIC_dll_debug}")
        else()
            add_library(libfabric::libfabric UNKNOWN IMPORTED)
            set_target_properties(libfabric::libfabric PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${LIBFABRIC_INCLUDE_DIRS}")
            if(EXISTS "${LIBFABRIC_LIBRARY}")
                set_target_properties(libfabric::libfabric PROPERTIES
                    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                    IMPORTED_LOCATION "${LIBFABRIC_LIBRARY}")
            endif()
            if(LIBFABRIC_LIBRARY_RELEASE)
                set_property(TARGET libfabric::libfabric APPEND PROPERTY
                    IMPORTED_CONFIGURATIONS RELEASE)
                set_target_properties(libfabric::libfabric PROPERTIES
                    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                    IMPORTED_LOCATION_RELEASE "${LIBFABRIC_LIBRARY_RELEASE}")
            endif()
            if(LIBFABRIC_LIBRARY_DEBUG)
                set_property(TARGET libfabric::libfabric APPEND PROPERTY
                    IMPORTED_CONFIGURATIONS DEBUG)
                set_target_properties(libfabric::libfabric PROPERTIES
                    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                    IMPORTED_LOCATION_DEBUG "${LIBFABRIC_LIBRARY_DEBUG}")
            endif()
        endif()
    endif()
endif()
