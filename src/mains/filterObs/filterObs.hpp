/*
 * (C) Copyright 2024 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#ifndef MAINS_FILTEROBS_H_
#define MAINS_FILTEROBS_H_

#include <algorithm>
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
#include "oops/util/TimeWindow.h"

#include "ioda/core/IodaUtils.h"
#include "ioda/ObsSpace.h"

// This application can be run standalone and will perform simple
// filtering operations. The input is a set of ioda obs files, and
// the output is a single ioda obs file with the results of applying
// the filtering operations.
//
// Currently, the time window filtering is the only filtering operation.

// -----------------------------------------------------------------------------
  /// \brief Implementation of the receipt time filter.
  /// \details This filter is primarily (solely?) for dealing with contrived
  /// data that is being created for demo or research purposes. The idea is to
  /// set an "accept window" representing the arrival time window and to
  /// reject the obs that are outside that window. Note that the parameters of
  /// the filter are: an accept window, and a variable name containing the
  /// receipt times for each location.
  /// \param varName variable name holding the receipt times
  /// \param acceptWindow keep locations with receipt times inside this window
  /// \param obsdb ObsSpace object that the filter is being applied to
  static int applyReceiptTimeFilter(const std::string & receiptVarName,
                                    const util::TimeWindow & acceptWindow,
                                    ioda::ObsSpace & obsdb) {
    // Reject all locations that have a datetime stamp (MetaData/dateTime)
    // that is outside the accept window.
    int numRejected = 0;

    // The calling function has checked that both parameters (acceptWindow and
    // receiptTimeVariable) exist in the YAML configuration. Check to make sure
    // the receiptTimeVariable exists, and if so apply the filter.
    std::string grpName;
    std::string varName;
    std::size_t slashPos = receiptVarName.find("/");
    if (slashPos != std::string::npos) {
      grpName = receiptVarName.substr(0, slashPos);
      varName = receiptVarName.substr(slashPos + 1);
    } else {
      grpName = "MetaData";
      varName = receiptVarName;
    }
    std::vector<util::DateTime> receiptTimes;
    if (obsdb.has(grpName, varName)) {
      obsdb.get_db(grpName, varName, receiptTimes);
    } else {
      std::string errMsg = std::string("Receipt time variable does not exist: ") +
          receiptVarName;
      throw eckit::BadParameter(errMsg, Here());
    }

    // Walk through the receiptTimes comparing those to the acceptWindow.
    // Construct a boolean vector with 'true' values in the positions
    // where the receiptTimes entry is inside the acceptWindow. This
    // boolean vector can then be handed off to the ObsSpace::reduce
    // function to remove the rejected locations.
    std::vector<bool> keepTheseLocs = acceptWindow.createTimeMask(receiptTimes);
    numRejected = std::count(keepTheseLocs.begin(), keepTheseLocs.end(), false);

    // Only call reduce if there were any locations that were rejected.
    if (numRejected > 0) {
      obsdb.reduce(keepTheseLocs);
    }
    return numRejected;
  }

namespace ioda {

template <typename OBS> class FilterObs : public oops::Application {
  typedef oops::ObsSpace<OBS>         ObsSpace_;

 public:
  // ---------------------------------------------------------------------------
  explicit FilterObs(const eckit::mpi::Comm & comm = oops::mpi::world()) : Application(comm) {}
  // ---------------------------------------------------------------------------
  virtual ~FilterObs() {}
  // ---------------------------------------------------------------------------
  int execute(const eckit::Configuration & fullConfig) const {
    //  Setup observation window
    const util::TimeWindow timeWindow(fullConfig.getSubConfiguration("time window"));
    oops::Log::info() << "Observation window: " << timeWindow << std::endl;

    // Grab config for the ObsSpace. Normally, obsdataout spec is
    // optional but in this case we want to make sure it is
    // included since we need to produce an output file.
    const eckit::LocalConfiguration obsconf(fullConfig, "obs space");
    oops::Log::debug() << "ObsSpace configuration is:" << obsconf << std::endl;
    if (!obsconf.has("obsdataout")) {
      std::string errMsg =
        std::string("ioda-filterObs: Must include 'obsdataout' spec ") +
        std::string("inside the 'obs space' spec");
      throw eckit::BadParameter(errMsg, Here());
    }

    // Create an ObsSpace object and the time window filtering will
    // happen automatically via the ioda reader.
    ObsSpace_ obsdb(obsconf, this->getComm(), timeWindow);

    // If specified, apply the receipt time filter - keep track of how many locations
    // were rejected due to the receipt time filter.
    int numReceiptTimeRejected = -1;
    const std::string receiptTimeFilterSpec("receipt time filter");
    if (fullConfig.has(receiptTimeFilterSpec)) {
      const eckit::LocalConfiguration filterConfig =
          fullConfig.getSubConfiguration(receiptTimeFilterSpec);
      const util::TimeWindow receiptAcceptWindow(
          filterConfig.getSubConfiguration("accept window"));
      const std::string receiptVarName = filterConfig.getString("variable name");
      numReceiptTimeRejected = applyReceiptTimeFilter(receiptVarName,
          receiptAcceptWindow, obsdb.obsspace());
    }

    // Display some stats
    oops::Log::info() << obsdb.obsname() << ": Total number of locations read: "
                      << obsdb.obsspace().sourceNumLocs() << std::endl;
    oops::Log::info() << obsdb.obsname() << ": Total number of locations kept: "
                      << obsdb.obsspace().globalNumLocs() << std::endl;
    oops::Log::info() << obsdb.obsname() << ": Number of locations outside time window: "
                      << obsdb.obsspace().globalNumLocsOutsideTimeWindow() << std::endl;
    if (numReceiptTimeRejected >= 0) {
      oops::Log::info() << obsdb.obsname()
                        << ": Number of locations rejected by the receipt time filter: "
                        << numReceiptTimeRejected << std::endl;
    }

    // Write the output file - already checked that we have an obsdataout spec
    obsdb.save();
    return 0;
  }

// -----------------------------------------------------------------------------
 private:
  std::string appname() const override {
    return "oops::FilterObs<" + OBS::name() + ">";
  }
};

}  // namespace ioda

#endif  // MAINS_FILTEROBS_H_
