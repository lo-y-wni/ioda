#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <string>
#include <gsl/gsl-lite.hpp>

#include "oops/util/parameters/OptionalParameter.h"
#include "oops/util/parameters/Parameters.h"

namespace ioda {
namespace Engines {
namespace ODC {

/// \brief Parameters controlling the behavior of a VariableReader.
class VariableReaderParametersBase : public oops::Parameters {
  OOPS_ABSTRACT_PARAMETERS(VariableReaderParametersBase, Parameters)

 public:
  /// \brief Type of the VariableReader to use.
  oops::OptionalParameter<std::string> type{"type", this};
};

/// \brief Reads values of an ioda variable from a column of a data table loaded from an ODB file.
///
/// Each subclass needs to typedef `Parameters_` to the type of a subclass of
/// VariableReaderParametersBase storing its configuration options, and to provide a constructor
/// with the following signature:
///
///     SubclassName(const Parameters_ &parameters, const std::string &column,
///                  const std::string &member, const DataFromSQL &sqlData);
///
/// where `parameters` are the reader's configuration options, `column` and `member` are the names
/// of the ODB column and (for bitfield columns) its member from which variable values should be
/// extracted, and `sqlData` is the source of these values: a data table loaded from an ODB file.
class VariableReaderBase {
public:
  virtual ~VariableReaderBase() {}

  /// \brief Read values of an integer-valued ioda variable at a specified location.
  ///
  /// \param[in] odbRowsAtLocation
  ///   Indices of all ODB rows associated with a particular location.
  /// \param[inout] valuesAtLocation
  ///   On input: a range filled with ODB missing values. On output, that range should be filled
  ///   with the values of all channels of the ioda variable at the location in question.
  virtual void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                           gsl::span<int> valuesAtLocation) const = 0;

  /// \overload for float-valued ioda variables.
  virtual void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                           gsl::span<float> valuesAtLocation) const = 0;

  /// \overload for string-valued ioda variables.
  ///
  /// \note On input, `valuesAtLocation` is filled with empty strings.
  virtual void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                           gsl::span<std::string> valuesAtLocation) const = 0;

  /// \overload for Boolean-valued ioda variables read from bitfield column members.
  ///
  /// \note On input, `valuesAtLocation` is filled with zeros.
  virtual void getVariableValuesAtLocation(gsl::span<const size_t> odbRowsAtLocation,
                                           gsl::span<char> valuesAtLocation) const = 0;
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
