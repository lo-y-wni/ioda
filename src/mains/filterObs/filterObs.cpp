/*
 * (C) Copyright 2024 UCAR.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 */

#include "./filterObs.hpp"

#include "oops/runs/Run.h"

#include "ioda/IodaTrait.h"

int main(int argc, char ** argv) {
  oops::Run run(argc, argv);
  ioda::FilterObs<ioda::IodaTrait> filter;
  return run.execute(filter);
}
