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

#include "filterObsParameters.hpp"

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

// -----------------------------------------------------------------------------
  /// \brief Implementation of the receipt time filter.
  /// \details This filter is primarily (solely?) for dealing with contrived
  /// data that is being created for demo or research purposes. The idea is to
  /// be able to set a cutoff time representing the current arrival time and to
  /// reject the obs that haven't "arrived" yet. Note that the parameters of
  /// the filter are: a cutoff time, and a variable name containing the
  /// receipt times for each location.
  /// \param params oops Parameters object for the receipt filter
  /// \param obsdb ObsSpace object that the filter is being applied to
  static int applyReceiptTimeFilter(const ioda::ReceiptTimeFilterParameters & params,
                                    ioda::ObsSpace & obsdb) {
    // Reject all locations that have a datetime stamp (MetaData/DateTime)
    // that is in the future from the cutoff datetime. These are locations
    // in a contrived case that haven't "arrived" yet.
    int numRejected = 0;

    // The parameter validation has checked that both parameters (cutoffTime and
    // receiptTimeVariable) exist and contain string values. Check to make sure
    // the receiptTimeVariable exists, and attempt to construct a DateTime object
    // from the cutoffTime value.
    util::DateTime cutoffTime(params.cutoffTime.value());

    std::string grpName;
    std::string varName;
    std::size_t slashPos = params.receiptTimeVariable.value().find("/");
    if (slashPos != std::string::npos) {
      grpName = params.receiptTimeVariable.value().substr(0, slashPos);
      varName = params.receiptTimeVariable.value().substr(slashPos + 1);
    } else {
      grpName = "MetaData";
      varName = params.receiptTimeVariable.value();
    }
    std::vector<util::DateTime> receiptTimes;
    if (obsdb.has(grpName, varName)) {
      obsdb.get_db(grpName, varName, receiptTimes);
    } else {
      std::string errMsg = std::string("Receipt time variable does not exist: ") +
                           params.receiptTimeVariable.value();
      throw eckit::BadParameter(errMsg, Here());
    }

    // Walk through the receiptTimes comparing those to the cutoffTime.
    // Construct a boolean vector with 'true' values in the positions
    // where the receiptTimes entry is <= to the cutoffTime. This
    // boolean vector can then be handed off to the ObsSpace::reduce
    // function to remove the rejected locations.
    std::vector<bool> keepTheseLocs(receiptTimes.size());
    for (std::size_t i = 0; i < receiptTimes.size(); ++i) {
      keepTheseLocs[i] = (receiptTimes[i] <= cutoffTime);
      if (!keepTheseLocs[i]) {
        numRejected++;
      }
    }

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
    eckit::LocalConfiguration obsconf(fullConfig, "obs space");
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
    std::string receiptTimeFilterSpec("receipt time filter");
    if (fullConfig.has(receiptTimeFilterSpec)) {
      ReceiptTimeFilterParameters params;
      params.validateAndDeserialize(fullConfig.getSubConfiguration(receiptTimeFilterSpec));
      numReceiptTimeRejected = applyReceiptTimeFilter(params, obsdb.obsspace());
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
                        << ": Number of locations beyond the receipt time cutoff: "
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
