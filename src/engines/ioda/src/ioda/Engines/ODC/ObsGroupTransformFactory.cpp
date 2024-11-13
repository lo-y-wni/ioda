/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/ObsGroupTransformFactory.h"

namespace ioda {
namespace Engines {
namespace ODC {

ObsGroupTransformFactory::ObsGroupTransformFactory(const std::string & name) {
  if (getMakers().find(name) != getMakers().end()) {
    throw std::runtime_error(name + " already registered in ObsGroupTransformFactory.");
  }
  getMakers()[name] = this;
}

std::unique_ptr<ObsGroupTransformBase>
ObsGroupTransformFactory::create(const ObsGroupTransformParametersBase &transformParameters,
                                 const ODC_Parameters &odcParameters,
                                 const OdbVariableCreationParameters &variableCreationParameters) {
  const std::string &id = transformParameters.name;
  typename std::map<std::string, ObsGroupTransformFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in ObsGroupTransformFactory.");
  }
  std::unique_ptr<ObsGroupTransformBase> ptr(it->second->make(transformParameters,
                                                              odcParameters,
                                                              variableCreationParameters));
  return ptr;
}

std::unique_ptr<ObsGroupTransformParametersBase>
ObsGroupTransformFactory::createParameters(const std::string &id) {
  typename std::map<std::string, ObsGroupTransformFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in ObsGroupTransformFactory");
  }
  std::unique_ptr<ObsGroupTransformParametersBase> ptr(it->second->makeParameters());
  return ptr;
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
