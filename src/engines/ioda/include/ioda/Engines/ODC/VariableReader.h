#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "VariableReaderBase.h"

#include "oops/util/parameters/RequiredParameter.h"

namespace ioda {
namespace Engines {
namespace ODC {

class DataFromSQL;

// -----------------------------------------------------------------------------

/// \brief Parameters controlling the behavior of VariableReaderFromRowsWithNonMissingValues
class VariableReaderFromRowsWithNonMissingValuesParameters : public VariableReaderParametersBase {
  OOPS_CONCRETE_PARAMETERS(VariableReaderFromRowsWithNonMissingValuesParameters,
                           VariableReaderParametersBase)
  // No extra parameters needed.
};

/// A subclass of VariableReader that reads the values of an n-channel ioda variable at a
/// given location from the first n ODB rows associated with that location that contain non-missing
/// values. If there are only m < n such rows, the last (n - m) channels remain filled with missing
/// values.
class VariableReaderFromRowsWithNonMissingValues : public VariableReaderBase {
public:
  typedef VariableReaderFromRowsWithNonMissingValuesParameters Parameters_;

  explicit VariableReaderFromRowsWithNonMissingValues(const Parameters_ &parameters,
                                                      const std::string &column,
                                                      const std::string &member,
                                                      const DataFromSQL &sqlData);

  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<int> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<float> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<std::string> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<char> valuesAtLocation) const override;

private:
  std::string member_;
  int columnIndex_;
  std::uint64_t bitfieldMask_ = 0;
  const DataFromSQL &sqlData_;
};

// -----------------------------------------------------------------------------

/// \brief Parameters controlling the behavior of VariableReaderFromRowsWithMatchingVarnos
class VariableReaderFromRowsWithMatchingVarnosParameters : public VariableReaderParametersBase {
  OOPS_CONCRETE_PARAMETERS(VariableReaderFromRowsWithMatchingVarnosParameters,
                           VariableReaderParametersBase)
public:
  /// \brief An ordered list of varnos. Variable values will be read only from rows containing these
  /// varnos.
  oops::RequiredParameter<std::vector<int>> varnos{"varnos", this};
};

/// A subclass of VariableReader that reads the values of all channels of an ioda variable at a
/// given location from the ODB rows associated with that location that contain the first varno
/// specified in the `varnos` parameter, then those that contain the second varno, and so on.
///
/// If the total number of such rows is less than the number of channels, the surplus channels
/// remain filled with missing values.
class VariableReaderFromRowsWithMatchingVarnos : public VariableReaderBase {
public:
  typedef VariableReaderFromRowsWithMatchingVarnosParameters Parameters_;

  explicit VariableReaderFromRowsWithMatchingVarnos(const Parameters_ &parameters,
                                                    const std::string &column,
                                                    const std::string &member,
                                                    const DataFromSQL &sqlData);

  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<int> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<float> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<std::string> valuesAtLocation) const override;
  void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                   gsl::span<char> valuesAtLocation) const override;

private:
  template <typename T>
  void getTypedVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                        gsl::span<T> valuesAtLocation) const;

  Parameters_ parameters_;
  std::string member_;
  int valueColumnIndex_;
  int varnoColumnIndex_;
  std::uint64_t bitfieldMask_ = 0;
  const DataFromSQL &sqlData_;
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
