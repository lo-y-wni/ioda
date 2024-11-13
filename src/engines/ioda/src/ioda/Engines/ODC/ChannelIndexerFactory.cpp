/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/ChannelIndexerFactory.h"

namespace ioda {
namespace Engines {
namespace ODC {

ChannelIndexerFactory::ChannelIndexerFactory(const std::string & name) {
  if (getMakers().find(name) != getMakers().end()) {
    throw std::runtime_error(name + " already registered in ChannelIndexerFactory.");
  }
  getMakers()[name] = this;
}

std::unique_ptr<ChannelIndexerBase>
ChannelIndexerFactory::create(const ChannelIndexerParametersBase & params) {
  const std::string &id = params.method;
  typename std::map<std::string, ChannelIndexerFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in ChannelIndexerFactory.");
  }
  std::unique_ptr<ChannelIndexerBase> ptr(it->second->make(params));
  return ptr;
}

std::unique_ptr<ChannelIndexerParametersBase>
ChannelIndexerFactory::createParameters(const std::string &id) {
  typename std::map<std::string, ChannelIndexerFactory*>::iterator it =
      getMakers().find(id);
  if (it == getMakers().end()) {
    throw std::runtime_error(id + " does not exist in ChannelIndexerFactory");
  }
  std::unique_ptr<ChannelIndexerParametersBase> ptr(it->second->makeParameters());
  return ptr;
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
