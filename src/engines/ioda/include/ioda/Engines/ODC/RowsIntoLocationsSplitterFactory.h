#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <vector>

#include "oops/util/AssociativeContainers.h"

#include "RowsIntoLocationsSplitterBase.h"

namespace ioda {
namespace Engines {
namespace ODC {

class RowsIntoLocationsSplitterFactory {
 public:
  /// \brief Create and return a new splitter.
  ///
  /// The splitter's type is determined by the `method` attribute of \p params.
  /// \p params must be an instance of the subclass of RowsIntoLocationsSplitterParametersBase
  /// associated with that splitter type, otherwise an exception will be thrown.
  static std::unique_ptr<RowsIntoLocationsSplitterBase> create(
      const RowsIntoLocationsSplitterParametersBase &params);

  /// \brief Create and return an instance of the RowsIntoLocationsSplitterParametersBase subclass
  /// storing parameters of splitters of the specified type.
  static std::unique_ptr<RowsIntoLocationsSplitterParametersBase> createParameters(
      const std::string &id);

  /// \brief Return the names of all splitters that can be created by one of the registered makers.
  static std::vector<std::string> getMakerNames() {
    return oops::keys(getMakers());
  }

  virtual ~RowsIntoLocationsSplitterFactory() = default;

 protected:
  /// \brief Register a maker able to create splitters of type \p id.
  explicit RowsIntoLocationsSplitterFactory(const std::string &id);

 private:
  virtual std::unique_ptr<RowsIntoLocationsSplitterBase> make(
      const RowsIntoLocationsSplitterParametersBase &) = 0;

  virtual std::unique_ptr<RowsIntoLocationsSplitterParametersBase> makeParameters() const = 0;

  static std::map<std::string, RowsIntoLocationsSplitterFactory*> & getMakers() {
    static std::map<std::string, RowsIntoLocationsSplitterFactory*> makers_;
    return makers_;
  }
};

// -----------------------------------------------------------------------------

/// \brief A subclass of RowsIntoLocationsSplitterFactory able to create instances of T
/// (a concrete subclass of RowsIntoLocationsSplitterBase).
template<class T>
class RowsIntoLocationsSplitterMaker : public RowsIntoLocationsSplitterFactory {
  typedef typename T::Parameters_ Parameters_;

  std::unique_ptr<RowsIntoLocationsSplitterBase> make(
      const RowsIntoLocationsSplitterParametersBase & parameters) override {
    const auto &stronglyTypedParameters = dynamic_cast<const Parameters_&>(parameters);
    return std::make_unique<T>(stronglyTypedParameters);
  }

  std::unique_ptr<RowsIntoLocationsSplitterParametersBase> makeParameters() const override {
    return std::make_unique<Parameters_>();
  }

 public:
  explicit RowsIntoLocationsSplitterMaker(const std::string & name) :
    RowsIntoLocationsSplitterFactory(name)
  {}
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
