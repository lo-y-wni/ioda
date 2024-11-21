# (C) Copyright 2024 UCAR.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.

# Filter ioda files

ecbuild_add_executable( TARGET  ioda-filterObs.x
                        SOURCES filterObs.cpp filterObs.hpp
                        LIBS    ioda )