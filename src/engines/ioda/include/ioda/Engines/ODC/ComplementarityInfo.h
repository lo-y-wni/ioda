#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <map>
#include <string>
#include <vector>

#include "OdbConstants.h"  // for OdbColumnType

namespace ioda {

namespace detail {
class ODBLayoutParameters;
}  // namespace detail

namespace Engines {
namespace ODC {

class OdbQueryParameters;

/// \brief Information about complementary ODB columns and ioda variables.
///
/// A single column in an ODB file can only hold strings at most eight characters long.
/// By convention, longer strings are split into eight-character chunks and stored in multiple
/// _complementary columns_; the names of these columns are obtained by appending an underscore
/// and a numerical suffix to the original column name (e.g. `site_name_1`, `site_name_2` etc.).
/// This class identifies such groups of complementary columns; it also constructs and keeps track
/// of the names of ioda variables into which these columns should initially be loaded, before
/// subsequent concatenation into single ioda variables.
class ComplementarityInfo {
public:
  /// \brief Constructor.
  ///
  /// Identifies groups of complementary columns present in an input ODB file and corresponding to
  /// individual columns listed in a mapping file and included in a query. Constructs names of
  /// temporary ioda variables into which these columns should be loaded.
  ///
  /// \param layoutParams
  ///   ODB layout parameters.
  /// \param queryParams
  ///   Query parameters.
  /// \param odbColumnsInfo
  ///   Maps qualified names of columns present in an ODB file (qualified = including table name)
  ///   to the types of these columns.
  ComplementarityInfo(
      const detail::ODBLayoutParameters &layoutParams,
      const OdbQueryParameters &queryParams,
      const std::map<std::string, OdbColumnType> &odbColumnsInfo);

  /// \brief Maps the name of each column that has been split into multiple complementary columns
  /// to the names of these columns.
  const std::map<std::string, std::vector<std::string>> &complementaryColumns() const {
    return complementaryColumns_;
  }

  /// \brief Maps the name of each ioda variable that should be built by rowwise concatenation of
  /// strings stored in multiple ioda variables to the names of these variables.
  const std::map<std::string, std::vector<std::string>> &complementaryVariables() const {
    return complementaryVariables_;
  }

private:
  static std::string makeQualifiedComponentColumnName(const std::string &columnName,
                                                      const std::string &tableName,
                                                      const int componentIndex);

  static std::string makeComponentVariablePath(const std::string &variableGroup,
                                               const std::string &variableName,
                                               const int componentIndex);

  std::map<std::string, std::vector<std::string>> complementaryColumns_;
  std::map<std::string, std::vector<std::string>> complementaryVariables_;
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
