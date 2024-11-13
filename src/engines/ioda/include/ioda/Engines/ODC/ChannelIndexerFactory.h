#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <vector>

#include "oops/util/AssociativeContainers.h"
#include "oops/util/parameters/Parameters.h"
#include "oops/util/parameters/PolymorphicParameter.h"

#include "ChannelIndexerBase.h"

namespace ioda {
namespace Engines {
namespace ODC {

class ChannelIndexerFactory {
 public:
  /// \brief Create and return a new indexer.
  ///
  /// The indexer's type is determined by the `method` attribute of \p params.
  /// \p params must be an instance of the subclass of ChannelIndexerParametersBase
  /// associated with that indexer type, otherwise an exception will be thrown.
  static std::unique_ptr<ChannelIndexerBase> create(
      const ChannelIndexerParametersBase &params);

  /// \brief Create and return an instance of the ChannelIndexerParametersBase subclass
  /// storing parameters of indexers of the specified type.
  static std::unique_ptr<ChannelIndexerParametersBase> createParameters(const std::string &id);

  /// \brief Return the names of all indexers that can be created by one of the registered makers.
  static std::vector<std::string> getMakerNames() {
    return oops::keys(getMakers());
  }

  virtual ~ChannelIndexerFactory() = default;

 protected:
  /// \brief Register a maker able to create indexers of type \p id.
  explicit ChannelIndexerFactory(const std::string &id);

 private:
  virtual std::unique_ptr<ChannelIndexerBase> make(
      const ChannelIndexerParametersBase &) = 0;

  virtual std::unique_ptr<ChannelIndexerParametersBase> makeParameters() const = 0;

  static std::map<std::string, ChannelIndexerFactory*> & getMakers() {
    static std::map<std::string, ChannelIndexerFactory*> makers_;
    return makers_;
  }
};

/// \brief A subclass of ChannelIndexerFactory able to create instances of T
/// (a concrete subclass of ChannelIndexerBase).
template<class T>
class ChannelIndexerMaker : public ChannelIndexerFactory {
  typedef typename T::Parameters_ Parameters_;

  std::unique_ptr<ChannelIndexerBase> make(
      const ChannelIndexerParametersBase & parameters) override {
    const auto &stronglyTypedParameters = dynamic_cast<const Parameters_&>(parameters);
    return std::make_unique<T>(stronglyTypedParameters);
  }

  std::unique_ptr<ChannelIndexerParametersBase> makeParameters() const override {
    return std::make_unique<Parameters_>();
  }

 public:
  explicit ChannelIndexerMaker(const std::string & name) :
    ChannelIndexerFactory(name)
  {}
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
