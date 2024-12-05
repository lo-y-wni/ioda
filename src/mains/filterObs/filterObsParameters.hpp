#pragma once
/*
 * (C) Copyright 2024 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "oops/util/parameters/Parameters.h"
#include "oops/util/parameters/OptionalParameter.h"
#include "oops/util/parameters/Parameter.h"
#include "oops/util/parameters/RequiredParameter.h"

namespace eckit {
  class Configuration;
}

namespace ioda {

class ReceiptTimeFilterParameters : public oops::Parameters {
    OOPS_CONCRETE_PARAMETERS(ReceiptTimeFilterParameters, oops::Parameters)

 public:
    /// Cutoff datetime. All locations with datetime stamps farther in
    /// the future will be rejected.
    ///
    /// This filter is used for processing contrived data, or for
    /// recreating a series of events from a prior forecast. The
    /// idea is to filter out the obs that haven't "arrived" yet.
    oops::RequiredParameter<std::string> cutoffTime {"cutoff time", this};

    /// ObsSpace variable name containing the receipt times
    oops::RequiredParameter<std::string> receiptTimeVariable {"variable name", this};
};
}  // namespace ioda
