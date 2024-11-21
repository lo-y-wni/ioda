/*
 * (C) Copyright 2024 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#ifndef MAINS_FILTEROBS_H_
#define MAINS_FILTEROBS_H_

#include <string>
#include <vector>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"

#include "oops/base/Observations.h"
#include "oops/base/ObsSpaces.h"
#include "oops/mpi/mpi.h"
#include "oops/runs/Application.h"
#include "oops/util/DateTime.h"
#include "oops/util/Duration.h"
#include "oops/util/Logger.h"

#include "ioda/core/IodaUtils.h"
#include "ioda/ObsSpace.h"

// This application can be run standalone and will perform simple
// filtering operations. The input is a set of ioda obs files, and
// the output is a single ioda obs file with the results of applying
// the filtering operations.
//
// Currently, the time window filtering is the only filtering operation.

namespace ioda {

template <typename OBS> class FilterObs : public oops::Application {
  typedef oops::ObsSpace<OBS>         ObsSpace_;

 public:
  // ---------------------------------------------------------------------------
  explicit FilterObs(const eckit::mpi::Comm & comm = oops::mpi::world()) : Application(comm) {}
  // ---------------------------------------------------------------------------
  virtual ~FilterObs() {}
  // ---------------------------------------------------------------------------
  int execute(const eckit::Configuration & fullConfig, bool /* validate */) const {
    //  Setup observation window
    const util::TimeWindow timeWindow(fullConfig.getSubConfiguration("time window"));
    oops::Log::info() << "Observation window: " << timeWindow << std::endl;

    // Grab config for the ObsSpace. Normally, obsdataout spec is
    // optional but in this case we want to make sure it is
    // included since we need to produce an output file.
    eckit::LocalConfiguration obsconf(fullConfig, "obs space");
    oops::Log::debug() << "ObsSpace configuration is:" << obsconf << std::endl;
    if (!obsconf.has("obsdataout")) {
      std::string errMsg =
        std::string("ioda-filterObs: Must include 'obsdataout' spec ") +
        std::string("inside the 'obs space' spec");
      throw eckit::BadParameter(errMsg, Here());
    }

    // Create an ObsSpace object and the time window filtering will
    // happen automatically via the ioda reader. The time window
    // filter will have been applied so only step left after
    // construction is to save the ObsSpace into an output file.
    ObsSpace_ obsdb(obsconf, this->getComm(), timeWindow);

    // Display some stats
    oops::Log::info() << obsdb.obsname() << ": Total number of locations read: "
                      << obsdb.obsspace().sourceNumLocs() << std::endl;
    oops::Log::info() << obsdb.obsname() << ": Total number of locations kept: "
                      << obsdb.obsspace().globalNumLocs() << std::endl;
    oops::Log::info() << obsdb.obsname() << ": Number of locations outside time window: "
                      << obsdb.obsspace().globalNumLocsOutsideTimeWindow() << std::endl;

    // Write the output file - already checked that we have an obsdataout spec
    obsdb.save();
    return 0;
  }

// -----------------------------------------------------------------------------
 private:
  std::string appname() const override {
    return "oops::FilterObs<" + OBS::name() + ">";
  }
// -----------------------------------------------------------------------------
};

}  // namespace ioda

#endif  // MAINS_FILTEROBS_H_
