#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <vector>

#include "oops/util/AssociativeContainers.h"

#include "ObsGroupTransformBase.h"

namespace ioda {
namespace Engines {
namespace ODC {

class OdbVariableCreationParameters;
struct ODC_Parameters;

class ObsGroupTransformFactory {
 public:
  /// \brief Create and return a new ObsGroup transform.
  ///
  /// The transform's type is determined by the `name` attribute of \p params.
  /// \p params must be an instance of the subclass of ObsGroupTransformParametersBase
  /// associated with that transform type, otherwise an exception will be thrown.
  static std::unique_ptr<ObsGroupTransformBase> create(
      const ObsGroupTransformParametersBase &transformParameters,
      const ODC_Parameters &odcParameters,
      const OdbVariableCreationParameters &variableCreationParameters);

  /// \brief Create and return an instance of the ObsGroupTransformParametersBase subclass
  /// storing parameters of transforms of the specified type.
  static std::unique_ptr<ObsGroupTransformParametersBase> createParameters(const std::string &id);

  /// \brief Return the names of all transforms that can be created by one of the registered makers.
  static std::vector<std::string> getMakerNames() {
    return oops::keys(getMakers());
  }

  virtual ~ObsGroupTransformFactory() = default;

 protected:
  /// \brief Register a maker able to create transforms of type \p id.
  explicit ObsGroupTransformFactory(const std::string &id);

 private:
  virtual std::unique_ptr<ObsGroupTransformBase> make(
      const ObsGroupTransformParametersBase &transformParameters,
      const ODC_Parameters &odcParameters,
      const OdbVariableCreationParameters &variableCreationParameters) = 0;

  virtual std::unique_ptr<ObsGroupTransformParametersBase> makeParameters() const = 0;

  static std::map<std::string, ObsGroupTransformFactory*> & getMakers() {
    static std::map<std::string, ObsGroupTransformFactory*> makers_;
    return makers_;
  }
};

/// \brief A subclass of ObsGroupTransformFactory able to create instances of T
/// (a concrete subclass of ObsGroupTransformBase).
template<class T>
class ObsGroupTransformMaker : public ObsGroupTransformFactory {
  typedef typename T::Parameters_ Parameters_;

  std::unique_ptr<ObsGroupTransformBase> make(
      const ObsGroupTransformParametersBase & transformParameters,
      const ODC_Parameters &odcParameters,
      const OdbVariableCreationParameters &variableCreationParameters) override {
    const auto &stronglyTypedParameters = dynamic_cast<const Parameters_&>(transformParameters);
    return std::make_unique<T>(stronglyTypedParameters, odcParameters, variableCreationParameters);
  }

  std::unique_ptr<ObsGroupTransformParametersBase> makeParameters() const override {
    return std::make_unique<Parameters_>();
  }

 public:
  explicit ObsGroupTransformMaker(const std::string & name) :
    ObsGroupTransformFactory(name)
  {}
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
