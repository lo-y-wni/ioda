/*
 * (C) Copyright 2017 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <boost/algorithm/string.hpp>

#include "distribution/DistributionFactory.h"
#include "distribution/RoundRobin.h"

namespace ioda {
// -----------------------------------------------------------------------------

Distribution * DistributionFactory::createDistribution(const std::string & method) {
  if (boost::iequals(method, "RoundRobin"))
    return new RoundRobin;
  else
    return NULL;
}

// -----------------------------------------------------------------------------

}  // namespace ioda
