/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/RowsIntoLocationsSplitterFactory.h"

namespace ioda {
namespace Engines {
namespace ODC {

RowsIntoLocationsSplitterFactory::RowsIntoLocationsSplitterFactory(const std::string & name) {
  if (getMakers().find(name) != getMakers().end()) {
    throw std::runtime_error(name + " already registered in RowsIntoLocationsSplitterFactory.");
  }
  getMakers()[name] = this;
}

std::unique_ptr<RowsIntoLocationsSplitterBase>
RowsIntoLocationsSplitterFactory::create(const RowsIntoLocationsSplitterParametersBase & params) {
  const std::string &id = params.method;
  typename std::map<std::string, RowsIntoLocationsSplitterFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in RowsIntoLocationsSplitterFactory.");
  }
  std::unique_ptr<RowsIntoLocationsSplitterBase> ptr(it->second->make(params));
  return ptr;
}

std::unique_ptr<RowsIntoLocationsSplitterParametersBase>
RowsIntoLocationsSplitterFactory::createParameters(const std::string &id) {
  typename std::map<std::string, RowsIntoLocationsSplitterFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in RowsIntoLocationsSplitterFactory");
  }
  std::unique_ptr<RowsIntoLocationsSplitterParametersBase> ptr(it->second->makeParameters());
  return ptr;
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
