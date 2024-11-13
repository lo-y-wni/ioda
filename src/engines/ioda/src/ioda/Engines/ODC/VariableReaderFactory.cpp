/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/VariableReaderFactory.h"

namespace ioda {
namespace Engines {
namespace ODC {

VariableReaderFactory::VariableReaderFactory(const std::string & name) {
  if (getMakers().find(name) != getMakers().end()) {
    throw std::runtime_error(name + " already registered in VariableReaderFactory.");
  }
  getMakers()[name] = this;
}

std::unique_ptr<VariableReaderBase>
VariableReaderFactory::create(const VariableReaderParametersBase & params,
                              const std::string &column, const std::string &member,
                              const DataFromSQL &sqlData) {
  const std::string &id = params.type.value().value();
  typename std::map<std::string, VariableReaderFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in VariableReaderFactory.");
  }
  std::unique_ptr<VariableReaderBase> ptr(it->second->make(params, column, member, sqlData));
  return ptr;
}

std::unique_ptr<VariableReaderParametersBase>
VariableReaderFactory::createParameters(const std::string &id) {
  typename std::map<std::string, VariableReaderFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in VariableReaderFactory");
  }
  std::unique_ptr<VariableReaderParametersBase> ptr(it->second->makeParameters());
  return ptr;
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
