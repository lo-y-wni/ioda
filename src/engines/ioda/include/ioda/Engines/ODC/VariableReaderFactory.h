#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <vector>

#include "ioda/Engines/ODC/VariableReaderBase.h"
#include "oops/util/AssociativeContainers.h"
#include "oops/util/parameters/Parameters.h"
#include "oops/util/parameters/RequiredPolymorphicParameter.h"

namespace ioda {
namespace Engines {
namespace ODC {

class DataFromSQL;

class VariableReaderFactory {
 public:
  /// \brief Create and return a new reader.
  ///
  /// The reader's type is determined by the `type` attribute of \p params.
  /// \p params must be an instance of the subclass of VariableReaderParametersBase
  /// associated with that reader type, otherwise an exception will be thrown.
  static std::unique_ptr<VariableReaderBase> create(
      const VariableReaderParametersBase &params, const std::string &column,
      const std::string &member, const DataFromSQL &sqlData);

  /// \brief Create and return an instance of the VariableReaderParametersBase subclass
  /// storing parameters of readers of the specified type.
  static std::unique_ptr<VariableReaderParametersBase> createParameters(const std::string &id);

  /// \brief Return the names of all readers that can be created by one of the registered makers.
  static std::vector<std::string> getMakerNames() {
    return oops::keys(getMakers());
  }

  virtual ~VariableReaderFactory() = default;

 protected:
  /// \brief Register a maker able to create readers of type \p id.
  explicit VariableReaderFactory(const std::string &id);

 private:
  virtual std::unique_ptr<VariableReaderBase> make(
      const VariableReaderParametersBase &parameters, const std::string &column,
      const std::string &member, const DataFromSQL &sqlData) = 0;

  virtual std::unique_ptr<VariableReaderParametersBase> makeParameters() const = 0;

  static std::map<std::string, VariableReaderFactory*> & getMakers() {
    static std::map<std::string, VariableReaderFactory*> makers_;
    return makers_;
  }
};

/// \brief A subclass of VariableReaderFactory able to create instances of T
/// (a concrete subclass of VariableReaderBase).
template<class T>
class VariableReaderMaker : public VariableReaderFactory {
  typedef typename T::Parameters_ Parameters_;

  std::unique_ptr<VariableReaderBase> make(
      const VariableReaderParametersBase & parameters, const std::string &column,
      const std::string &member, const DataFromSQL &sqlData) override {
    const auto &stronglyTypedParameters = dynamic_cast<const Parameters_&>(parameters);
    return std::make_unique<T>(stronglyTypedParameters, column, member, sqlData);
  }

  std::unique_ptr<VariableReaderParametersBase> makeParameters() const override {
    return std::make_unique<Parameters_>();
  }

 public:
  explicit VariableReaderMaker(const std::string & name) :
    VariableReaderFactory(name)
  {}
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
